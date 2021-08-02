# Code for reading data logs produced by data_logger.py
#
# Copyright (C) 2021  Kevin O'Connor <kevin@koconnor.net>
#
# This file may be distributed under the terms of the GNU GPLv3 license.
import json, zlib

class error(Exception):
    pass


######################################################################
# Log data handlers
######################################################################

# Log data handlers: {name: class, ...}
LogHandlers = {}

# Extract requested position, velocity, and accel from a trapq log
class HandleTrapQ:
    ParametersMsgId = 2
    ParametersTotal = 3
    def __init__(self, lmanager, msg_id):
        self.msg_id = msg_id
        self.jdispatch = lmanager.get_jdispatch()
        self.cur_data = [(0., 0., 0., 0., (0., 0., 0.), (0., 0., 0.))]
        self.data_pos = 0
    def get_description(self, name_parts):
        ptypes = {}
        ptypes['velocity'] = {
            'label': '%s velocity' % (name_parts[1],),
            'units': 'Velocity\n(mm/s)', 'func': self.pull_velocity
        }
        ptypes['accel'] = {
            'label': '%s acceleration' % (name_parts[1],),
            'units': 'Acceleration\n(mm/s^2)', 'func': self.pull_accel
        }
        for axis, name in enumerate("xyz"):
            ptypes['axis_%s' % (name,)] = {
                'label': '%s axis %s position' % (name_parts[1], name),
                'units': 'Position\n(mm)',
                'func': (lambda t, a=axis: self.pull_axis_position(t, a))
            }
            ptypes['axis_%s_velocity' % (name,)] = {
                'label': '%s axis %s velocity' % (name_parts[1], name),
                'units': 'Velocity\n(mm/s)',
                'func': (lambda t, a=axis: self.pull_axis_velocity(t, a))
            }
            ptypes['axis_%s_accel' % (name,)] = {
                'label': '%s axis %s acceleration' % (name_parts[1], name),
                'units': 'Acceleration\n(mm/s^2)',
                'func': (lambda t, a=axis: self.pull_axis_accel(t, a))
            }
        pinfo = ptypes.get(name_parts[2])
        if pinfo is None:
            raise error("Unknown trapq data selection '%s'" % (name_parts[0],))
        return pinfo
    def _find_move(self, req_time):
        data_pos = self.data_pos
        while 1:
            move = self.cur_data[data_pos]
            print_time, move_t, start_v, accel, start_pos, axes_r = move
            if req_time <= print_time + move_t:
                mtime = max(0., min(move_t, req_time - print_time))
                return move, mtime
            data_pos += 1
            if data_pos < len(self.cur_data):
                self.data_pos = data_pos
                continue
            jmsg = self.jdispatch.pull_msg(req_time, self.msg_id)
            if jmsg is None:
                return move, move_t
            self.cur_data = jmsg['data']
            self.data_pos = data_pos = 0
    def pull_axis_position(self, req_time, axis):
        move, mtime = self._find_move(req_time)
        print_time, move_t, start_v, accel, start_pos, axes_r = move
        dist = (start_v + .5 * accel * mtime) * mtime;
        return start_pos[axis] + axes_r[axis] * dist
    def pull_axis_velocity(self, req_time, axis):
        move, mtime = self._find_move(req_time)
        print_time, move_t, start_v, accel, start_pos, axes_r = move
        if mtime >= move_t:
            return 0.
        return (start_v + accel * mtime) * axes_r[axis]
    def pull_axis_accel(self, req_time, axis):
        move, mtime = self._find_move(req_time)
        print_time, move_t, start_v, accel, start_pos, axes_r = move
        if mtime >= move_t:
            return 0.
        return accel * axes_r[axis]
    def pull_velocity(self, req_time):
        move, mtime = self._find_move(req_time)
        print_time, move_t, start_v, accel, start_pos, axes_r = move
        if mtime >= move_t:
            return 0.
        return start_v + accel * mtime
    def pull_accel(self, req_time):
        move, mtime = self._find_move(req_time)
        print_time, move_t, start_v, accel, start_pos, axes_r = move
        if mtime >= move_t:
            return 0.
        return accel
LogHandlers["trapq"] = HandleTrapQ

# Extract positions from queue_step log
class HandleStepQ:
    ParametersMsgId = 2
    ParametersTotal = 2
    def __init__(self, lmanager, msg_id):
        self.msg_id = msg_id
        self.jdispatch = lmanager.get_jdispatch()
        self.next_step_time = 0.
        self.next_step_clock = 0
        self.next_step_pos = self.last_step_pos = 0.
        self.cur_data = []
        self.data_pos = self.qs_interval = self.qs_count = self.qs_add = 0
        self.step_dist = self.qs_dist = self.prev_qs_dist = 0.

        self.slope_start_time = self.slope_end_time = 0.
        self.slope_start_pos = self.slope = 0.

        self.block_first_clock = 0
        self.block_first_time = self.block_last_time = self.inv_freq = 0.
    def get_description(self, name_parts):
        return {'label': '%s position' % (name_parts[1],),
                'units': 'Position\n(mm)', 'func': self.pull_position}
    def pull_position(self, req_time):
        smtime = .005
        while 1:
            if req_time <= self.slope_end_time:
                tdiff = req_time - self.slope_start_time
                return self.slope_start_pos + tdiff * self.slope
            if req_time <= self.next_step_time:
                check_time = self.next_step_time - smtime
                if req_time < check_time:
                    self.slope_start_pos = self.last_step_pos
                    self.slope_end_time = check_time
                    self.slope = 0.
                    continue
                self.slope_end_time = self.next_step_time
                self.slope_start_time = check_time
                self.slope = .5 * self.qs_dist / smtime
                self.slope_start_pos = self.last_step_pos
                continue
            if self.qs_count:
                self.last_step_pos = self.next_step_pos
                self.qs_count -= 1
                self.next_step_clock += self.qs_interval
                self.qs_interval += self.qs_add
                last_step_time = self.next_step_time
                cdiff = self.next_step_clock - self.block_first_clock
                tdiff = cdiff * self.inv_freq
                self.next_step_time = self.block_first_time + tdiff
                self.next_step_pos += self.qs_dist

                tdiff = self.next_step_time - last_step_time
                if tdiff <= 2. * smtime:
                    self.slope_end_time = self.next_step_time
                    self.slope = .5 * (self.prev_qs_dist + self.qs_dist) / tdiff
                else:
                    self.slope_end_time = last_step_time + smtime
                    self.slope = .5 * self.prev_qs_dist / smtime
                self.slope_start_time = last_step_time
                self.slope_start_pos = self.last_step_pos - .5*self.prev_qs_dist
                self.prev_qs_dist = self.qs_dist

                continue
            # Must load and calc next step
            if req_time > self.block_last_time:
                # Read next data block
                jmsg = self.jdispatch.pull_msg(req_time, self.msg_id)
                if jmsg is None:
                    return self.next_step_pos
                self.block_first_clock = jmsg['first_clock']
                self.block_first_time = jmsg['first_step_time']
                self.block_last_time = jmsg['last_step_time']
                cdiff = jmsg['last_clock'] - self.block_first_clock
                tdiff = self.block_last_time - self.block_first_time
                inv_freq = 0.
                if cdiff:
                    inv_freq = tdiff / cdiff
                self.inv_freq = inv_freq
                self.cur_data = jmsg['data']
                self.data_pos = 0
                self.step_dist = jmsg['step_distance']
                first_interval = self.cur_data[0][0]
                self.next_step_clock = self.block_first_clock - first_interval
                self.next_step_pos = jmsg['start_position']
                continue
            if self.data_pos >= len(self.cur_data):
                return self.next_step_pos
            self.qs_interval, count, self.qs_add = self.cur_data[self.data_pos]
            step_dist = self.step_dist
            if count < 0:
                count = -count
                step_dist = -step_dist
            self.qs_count = count
            self.qs_dist = step_dist
            self.data_pos += 1
LogHandlers["stepq"] = HandleStepQ


######################################################################
# Log reading
######################################################################

# Read, uncompress, and parse messages in a log built by data_logger.py
class JsonLogReader:
    def __init__(self, filename):
        self.file = open(filename, "rb")
        self.comp = zlib.decompressobj(31)
        self.msgs = [b""]
    def seek(self, pos):
        self.file.seek(pos)
        self.comp = zlib.decompressobj(-15)
    def pull_msg(self):
        msgs = self.msgs
        while 1:
            if len(msgs) > 1:
                msg = msgs.pop(0)
                try:
                    json_msg = json.loads(msg)
                except:
                    logging.exception("Unable to parse line")
                    continue
                return json_msg
            raw_data = self.file.read(8192)
            if not raw_data:
                return None
            data = self.comp.decompress(raw_data)
            parts = data.split(b'\x03')
            parts[0] = msgs[0] + parts[0]
            self.msgs = msgs = parts

# Store messages in per-subscription queues until handlers are ready for them
class JsonDispatcher:
    def __init__(self, log_prefix):
        self.queues = {'status': []}
        self.last_read_time = 0.
        self.log_reader = JsonLogReader(log_prefix + ".json.gz")
        self.is_eof = False
    def check_end_of_data(self):
        return self.is_eof and not any(self.queues.values())
    def add_handler(self, msg_id):
        if msg_id not in self.queues:
            self.queues[msg_id] = []
    def pull_msg(self, req_time, msg_id):
        q = self.queues[msg_id]
        while 1:
            if q:
                return q.pop(0)
            if req_time + 1. < self.last_read_time:
                return None
            json_msg = self.log_reader.pull_msg()
            if json_msg is None:
                self.is_eof = True
                return None
            qid = json_msg.get('q')
            mq = self.queues.get(qid)
            if mq is None:
                continue
            params = json_msg['params']
            mq.append(params)
            if qid == 'status':
                pt = json_msg.get('toolhead', {}).get('estimated_print_time')
                if pt is not None:
                    self.last_read_time = pt

# Main log access management
class LogManager:
    error = error
    def __init__(self, log_prefix):
        self.index_reader = JsonLogReader(log_prefix + ".index.gz")
        self.jdispatch = JsonDispatcher(log_prefix)
        self.initial_start_time = self.start_time = 0.
        self.active_handlers = {}
        self.initial_status = {}
        self.status = {}
    def setup_index(self):
        fmsg = self.index_reader.pull_msg()
        self.initial_status = status = fmsg['status']
        self.status = dict(status)
        start_time = status['toolhead']['estimated_print_time']
        self.initial_start_time = self.start_time = start_time
    def get_initial_status(self):
        return self.initial_status
    def available_datasets(self):
        return {name: None for name in LogHandlers}
    def get_jdispatch(self):
        return self.jdispatch
    def seek_time(self, req_time):
        self.start_time = req_start_time = self.initial_start_time + req_time
        seek_time = max(self.initial_start_time, req_start_time - 1.)
        file_position = 0
        while 1:
            fmsg = self.index_reader.pull_msg()
            if fmsg is None:
                break
            th = fmsg['status']['toolhead']
            ptime = max(th['estimated_print_time'], th.get('print_time', 0.))
            if ptime > seek_time:
                break
            file_position = fmsg['file_position']
        if file_position:
            self.jdispatch.log_reader.seek(file_position)
    def get_start_time(self):
        return self.start_time
    def setup_dataset(self, name):
        parts = name.split(':')
        cls = LogHandlers.get(parts[0])
        if cls is None:
            raise error("Unknown dataset '%s'" % (parts[0],))
        if len(parts) != cls.ParametersTotal:
            raise error("Invalid number of parameters for %s" % (parts[0],))
        msg_id = ":".join(parts[:cls.ParametersMsgId])
        hdl = self.active_handlers.get(msg_id)
        if hdl is None:
            self.active_handlers[msg_id] = hdl = cls(self, msg_id)
            self.jdispatch.add_handler(msg_id)
        return hdl.get_description(parts)

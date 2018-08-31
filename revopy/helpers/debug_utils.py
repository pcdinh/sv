# -*- coding: utf-8 -*-

import sys
import traceback
from traceback import StackSummary


def extract_exception_plus():
    """Get the usual traceback information, followed by a listing of all the
    local variables in each frame.
    """
    lines = []
    exc_info = sys.exc_info()
    tb = exc_info[2]
    stack = []
    while tb:
        f = tb.tb_frame
        while f:
            stack.append(f)
            f = f.f_back
        tb = tb.tb_next
    for line in traceback.TracebackException(
            type(exc_info[1]), exc_info[1], tb, limit=None).format(chain=True):
        lines.append(line)
    for frame in stack:
        if frame.f_code.co_name == "<module>":  # so it does not dump globals
            continue
        lines.append("Frame %s in %s at line %s" % (
                frame.f_code.co_name, frame.f_code.co_filename, frame.f_lineno
            )
        )
        for key, value in frame.f_locals.items():
            strx = "\t%20s = " % key
            # We have to be careful not to cause a new error in our error
            # printer! Calling str() on an unknown object could cause an
            # error we don't want.
            try:
                strx += str(value)
            except Exception:
                strx += "<ERROR WHILE PRINTING VALUE>, "
            lines.append(strx)
    return lines


def get_exception_details():
    from traceback import walk_tb, StackSummary, FrameSummary
    from time import strftime
    cla, exc, exc_traceback = sys.exc_info()
    exc_args = exc.__dict__["args"] if "args" in exc.__dict__ else "<no args>"
    ex_title = cla.__name__ + ": Exception:" + str(exc) + " - args:" + str(exc_args)
    msgs = [ex_title, ]
    except_location = ""
    tb: list[FrameSummary] = StackSummary.extract(walk_tb(exc_traceback), limit=None, capture_locals=True)
    for frame in tb:
        local_vars_info = []
        if frame.locals:
            for name, value in frame.locals.items():
                if name == "self":
                    continue
                local_vars_info.append(f'\t{name} = {value}')
        except_location += "\n" + frame.filename + ":" + str(frame.lineno) + " \n" + str(frame.name) + \
                           "\n<Args>:" + "\n".join(local_vars_info)
    msgs.insert(1, except_location)
    time = strftime("%Y-%m-%d %H:%M:%S")
    return time + " - " + ("".join(msgs))

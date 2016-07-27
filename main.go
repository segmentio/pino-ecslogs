package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/ecs-logs-go"
)

func main() {
	r := bufio.NewReader(os.Stdin)
	w := bufio.NewWriter(os.Stdout)

	dec := json.NewDecoder(r)
	dec.UseNumber()

	enc := json.NewEncoder(w)
	m := make(map[string]interface{})

	for {
		if ev, err := decode(dec, m); err != nil {
			break
		} else if err = encode(enc, ev); err != nil {
			break
		} else if err = w.Flush(); err != nil {
			break
		}
	}
}

func decode(dec *json.Decoder, obj map[string]interface{}) (ev ecslogs.Event, err error) {
	for k := range obj {
		delete(obj, k)
	}

	if err = dec.Decode(&obj); err != nil {
		switch err.(type) {
		case *json.InvalidUnmarshalError, *json.SyntaxError, *json.UnmarshalFieldError, *json.UnmarshalTypeError:
			host, _ := os.Hostname()
			ev = ecslogs.Event{
				Level: ecslogs.ALERT,
				Info: ecslogs.EventInfo{
					Host:   host,
					PID:    os.Getpid(),
					UID:    os.Getuid(),
					GID:    os.Getgid(),
					Errors: []ecslogs.EventError{ecslogs.MakeEventError(err)},
				},
				Data:    ecslogs.EventData{},
				Message: err.Error(),
			}
			err = nil
		}
		return
	}

	ev.Level = ecslogs.ALERT
	ev.Data = ecslogs.EventData{}

	for k, v := range obj {
		switch k {
		case "pid":
			ev.Info.PID = makeEventInfoPID(v)

		case "hostname":
			ev.Info.Host = makeEventInfoHost(v)

		case "level":
			ev.Level = makeEventLevel(v)

		case "time":
			ev.Time = makeEventTime(v)

		case "msg":
			ev.Message = makeEventMessage(v)

		case "v":
			// ?? not sure if it's worth checking the version number.

		case "type":
			// This is always "Error" when the message contains a stack trace.

		case "stack":
			ev.Info.Errors = []ecslogs.EventError{makeEventError(v)}

		default:
			ev.Data[k] = v
		}
	}

	return
}

func encode(enc *json.Encoder, ev ecslogs.Event) (err error) {
	if err = enc.Encode(ev); err != nil {
		switch err.(type) {
		case *json.UnsupportedTypeError, *json.UnsupportedValueError:
			err = enc.Encode(ecslogs.Event{
				Level: ecslogs.ALERT,
				Info: ecslogs.EventInfo{
					Host:   ev.Info.Host,
					Source: ev.Info.Source,
					PID:    ev.Info.PID,
					UID:    ev.Info.UID,
					GID:    ev.Info.GID,
					Errors: []ecslogs.EventError{ecslogs.MakeEventError(err)},
				},
				Message: fmt.Sprintf("%#v", ev),
			})
		}
	}
	return
}

func makeEventInfoPID(v interface{}) (pid int) {
	if x, ok := v.(json.Number); ok {
		pid, _ = strconv.Atoi(string(x))
	}
	return
}

func makeEventInfoHost(v interface{}) (host string) {
	if x, ok := v.(string); ok {
		host = x
	}
	return
}

func makeEventLevel(v interface{}) (level ecslogs.Level) {
	if x, ok := v.(json.Number); ok {
		switch lvl, _ := strconv.Atoi(string(x)); lvl {
		case 60:
			level = ecslogs.ALERT
		case 50:
			level = ecslogs.ERROR
		case 40:
			level = ecslogs.WARN
		case 30:
			level = ecslogs.INFO
		case 20:
			level = ecslogs.DEBUG
		case 10:
			level = ecslogs.DEBUG
		default:
			level = ecslogs.NOTICE
		}
	}
	return
}

func makeEventTime(v interface{}) (timestamp time.Time) {
	if x, ok := v.(json.Number); ok {
		t, _ := strconv.Atoi(string(x))
		timestamp = time.Unix(
			int64(t/1000),
			int64((t%1000)*1000000),
		)
	}
	return
}

func makeEventMessage(v interface{}) (message string) {
	if x, ok := v.(string); ok {
		message = x
	} else {
		message = fmt.Sprint(v)
	}
	return
}

func makeEventError(v interface{}) (error ecslogs.EventError) {
	if x, ok := v.(string); ok {
		trace := makeEventErrorTrace(x)
		error.Type = makeEventErrorType(trace)
		error.Error = makeEventErrorValue(trace)
		error.Stack = makeEventErrorStack(trace)
	}
	return
}

func makeEventErrorTrace(v string) (trace []string) {
	trace = strings.Split(v, "\n")

	for i, s := range trace {
		trace[i] = strings.TrimSpace(s)
	}

	return
}

func makeEventErrorType(trace []string) (errtype string) {
	if len(trace) != 0 {
		head := trace[0]
		if off := strings.IndexByte(head, ':'); off >= 0 {
			errtype = strings.TrimSpace(head[:off])
		} else {
			errtype = "Error"
		}
	}
	return
}

func makeEventErrorValue(trace []string) (errval string) {
	if len(trace) != 0 {
		head := trace[0]
		if off := strings.IndexByte(head, ':'); off >= 0 {
			errval = strings.TrimSpace(head[off+1:])
		}
	}
	return
}

func makeEventErrorStack(trace []string) (stack []interface{}) {
	if len(trace) > 1 {
		stack = make([]interface{}, len(trace)-1)

		for i, s := range trace[1:] {
			stack[i] = s
		}
	}
	return
}

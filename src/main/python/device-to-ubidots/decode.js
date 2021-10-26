/*
 * This script allows you to test a ttn v3 decoder.
 *
 * Put the decoder source in a file in this directory,
 * exactly as it will be pasted into the ttn v3 console.
 *
 * Create a JSON file in this directory that contains an
 * array of ttn v2 and/or v3 messages.
 *
 * Run this script with the command "node decode.js [decoder] [msgs json]".
 * eg: node decode.js axioma.js ./axioma_msgs.json
 *
 * The ./ in front of the messages filename is important
 * because of how the require function works.
 */
var fs = require('fs');
const { exit } = require('process');


var decoderScript = "./decoder.js";
var msgsJson = "./msgs.json";

if (process.argv.length > 2) {
    decoderScript = process.argv[2];
}

if (process.argv.length > 3) {
    msgsJson = process.argv[3];
}

var msgs = require(msgsJson);
var data = "";

try {
  data = fs.readFileSync(decoderScript, 'utf8')
} catch (err) {
  console.error(err)
  exit(1)
}

// Evaluating the source code read from decoder.js creates the decodeUplink
// function so it can be called.
eval(data);

// The output is so simple we can just write the JSON directly rather than
// creating an object and calling JSON.stringify();
//
// We might want to re-think that so we can write log messages.
console.log("[");

lastIdx = msgs.length - 1;
for (var i = 0; i < msgs.length; i++) {
    var port = -1;
    var payload_raw = "";
    var ts = "";

    
    var msgOk = false;

    if (msgs[i].hasOwnProperty("port") && msgs[i].hasOwnProperty("payload_raw")) {
        port = msgs[i].port;
        payload_raw = msgs[i].payload_raw;
        ts = msgs[i].metadata.time;
        msgOk = true;
    } else {
        if (msgs[i].hasOwnProperty("uplink_message")) {
            var uplink_message = msgs[i].uplink_message;
            if (uplink_message.hasOwnProperty("frm_payload") && uplink_message.hasOwnProperty("f_port") && uplink_message.hasOwnProperty("received_at")) {
                port = uplink_message.f_port;
                payload_raw = uplink_message.frm_payload;
                ts = uplink_message.received_at;
                msgOk = true;
            }
        }
    }

    if (msgOk === false) {
        continue;
    }

    try {
        var input = {
            bytes: Buffer.from(payload_raw, 'base64'),
            fPort: port
        };

		try {
        	var val = Decoder(input);
		}
		catch {
        	var val = decodeUplink(input);
		}
			
        // Only process messages that decoded properly.
        if (val.hasOwnProperty("data")) {
            var eol = i < lastIdx ? "," : "";
            var s = `{"ts":${Date.parse(ts)},`
            var first = true;
            for (var z of Object.keys(val["data"])) {
                if (first !== true) {
                    s += ",";
                }
                first = false;

                s += `"${z}":`;
                var v = val["data"][z];
                s += `"${v}"`;
            }
            s += "}";
            s += eol;
            console.log(s);
        }
    } catch (ex) {
        // Ignore it, probably an empty payload_raw or frm_payload in the source message.
    }
}

console.log("]");

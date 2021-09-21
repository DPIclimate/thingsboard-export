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
 * The ./ in front of the messages filename is important.
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

for (var i = 0; i < msgs.length; i++) {
    var port = -1;
    var payload_raw = "";
    var ts = "";
    
    if (msgs[i].hasOwnProperty("payload_raw")) {
        port = msgs[i].port;
        payload_raw = msgs[i].payload_raw;
        ts = msgs[i].metadata.time;
    } else if (msgs[i].hasOwnProperty("received_at")) {
        if ( ! msgs[i].uplink_message.hasOwnProperty("f_port")) {
            continue;
        }
        port = msgs[i].uplink_message.f_port;
        payload_raw = msgs[i].uplink_message.frm_payload;
        ts = msgs[i].received_at;
    }

    if (port === -1) {
        continue;
    }

    var input = {
        bytes: Buffer.from(payload_raw, 'base64'),
        fPort: port
    };

    var val = decodeUplink(input);
    console.log(`${ts}: ${JSON.stringify(val)}`);
}

/*
 * This script allows you to test a ttn v3 decoder.
 *
 * Put the decoder source in a file called decoder.js in this directory,
 * exactly as it will be pasted into the ttn v3 console.
 *
 * Create a file called msgs.json which contains an array of ttn v2 and/or v3 messages.
 *
 * Run this script with the command "node decode.js".
 */
var fs = require('fs');
const { exit } = require('process');

var msgs = require("./msgs.json");

var data = "";

try {
  data = fs.readFileSync('./decoder.js', 'utf8')
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
    
    if (msgs[i].hasOwnProperty("payload_raw")) {
        port = msgs[i].port;
        payload_raw = msgs[i].payload_raw;
    } else if (msgs[i].hasOwnProperty("received_at")) {
        port = msgs[i].uplink_message.f_port;
        payload_raw = msgs[i].uplink_message.frm_payload;
    }

    if (port === -1) {
        continue;
    }

    var input = {
        bytes: Buffer.from(payload_raw, 'base64'),
        fPort: port
    };

    var val = decodeUplink(input);
    console.log(val);
}

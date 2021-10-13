function Bytes2Float32(bytes) {
	var sign = (bytes & 0x80000000) ? -1 : 1;
	var exponent = ((bytes >> 23) & 0xFF) - 127;
	var significand = (bytes & ~(-1 << 23));
	if (exponent == 128) 
		return sign * ((significand) ? Number.NaN : Number.POSITIVE_INFINITY);
	if (exponent == -127) {
		if (significand === 0) return sign * 0.0;
		exponent = -126;
		significand /= (1 << 22);
	} else significand = (significand | (1 << 23)) / (1 << 23);
	return sign * significand * Math.pow(2, exponent);
}


function decodeUplink(bytes, port) {
	var payload = input.bytes;
	var RTC = (payload[0]<<24 | payload[1]<<16 | payload[2]<<8 | payload [3]);
	var batmv = (payload[4]<<8 | payload[5]);
	var solmv = (payload[6]<<8 | payload[7]);
	var command = (payload[9]);
	if (command === 0) {
		var moisture1 = (Math.round(Bytes2Float32(payload[10]<<24 | payload[11]<<16 | payload[12]<<8 | payload[13]<<0)*100)/100);
		var moisture2 = (Math.round(Bytes2Float32(payload[14]<<24 | payload[15]<<16 | payload[16]<<8 | payload[17]<<0)*100)/100);
		var moisture3 = (Math.round(Bytes2Float32(payload[18]<<24 | payload[19]<<16 | payload[20]<<8 | payload[21]<<0)*100)/100);
		var moisture4 = (Math.round(Bytes2Float32(payload[22]<<24 | payload[23]<<16 | payload[24]<<8 | payload[25]<<0)*100)/100);
		var moisture5 = (Math.round(Bytes2Float32(payload[26]<<24 | payload[27]<<16 | payload[28]<<8 | payload[29]<<0)*100)/100);
		var moisture6 = (Math.round(Bytes2Float32(payload[30]<<24 | payload[31]<<16 | payload[32]<<8 | payload[33]<<0)*100)/100);
		var moisture7 = (Math.round(Bytes2Float32(payload[34]<<24 | payload[35]<<16 | payload[36]<<8 | payload[37]<<0)*100)/100);
		var moisture8 = (Math.round(Bytes2Float32(payload[38]<<24 | payload[39]<<16 | payload[40]<<8 | payload[41]<<0)*100)/100);
		return {
			data: {
				"rtc": RTC,
				"solmv": solmv,
				"command": command,
				"moisture1": moisture1,
				"moisture2": moisture2,
				"moisture3": moisture3,
				"moisture4": moisture4,
				"moisture5": moisture5,
				"moisture6": moisture6,
				"moisture7": moisture7,
				"moisture8": moisture8
			}
		};
	} else if (command == 1) {
		var temperature1 = (Math.round(Bytes2Float32(payload[10]<<24 | payload[11]<<16 | payload[12]<<8 | payload[13]<<0)*100)/100);
		var temperature2 = (Math.round(Bytes2Float32(payload[14]<<24 | payload[15]<<16 | payload[16]<<8 | payload[17]<<0)*100)/100);
		var temperature3 = (Math.round(Bytes2Float32(payload[18]<<24 | payload[19]<<16 | payload[20]<<8 | payload[21]<<0)*100)/100);
		var temperature4 = (Math.round(Bytes2Float32(payload[22]<<24 | payload[23]<<16 | payload[24]<<8 | payload[25]<<0)*100)/100);
		var temperature5 = (Math.round(Bytes2Float32(payload[26]<<24 | payload[27]<<16 | payload[28]<<8 | payload[29]<<0)*100)/100);
		var temperature6 = (Math.round(Bytes2Float32(payload[30]<<24 | payload[31]<<16 | payload[32]<<8 | payload[33]<<0)*100)/100);
		var temperature7 = (Math.round(Bytes2Float32(payload[34]<<24 | payload[35]<<16 | payload[36]<<8 | payload[37]<<0)*100)/100);
		var temperature8 = (Math.round(Bytes2Float32(payload[38]<<24 | payload[39]<<16 | payload[40]<<8 | payload[41]<<0)*100)/100);
		return {
			data: {
				"rtc": RTC,
				"solmv": solmv,
				"command": command,
				"temperature1": temperature1,
				"temperature2": temperature2,
				"temperature3": temperature3,
				"temperature4": temperature4,
				"temperature5": temperature5,
				"temperature6": temperature6,
				"temperature7": temperature7,
				"temperature8": temperature8
			}
		};
	}

}

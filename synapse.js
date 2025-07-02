import WebSocket from 'ws';
import Zlib from './zlib.js';
import { createClient } from 'redis';
import { performance } from 'perf_hooks';
import fs from 'fs';

let objSocket = null;
let baOldData = null;

const redisClient = createClient({
  socket: {
    host: '172.24.169.200',
    port: 6379
  }
});

redisClient.on('error', (err) => {
  console.error('Redis Client Error', err);
});

await redisClient.connect();
console.log('Redis client connected');

const timingSummary = {
  connectStart: 0,
  connectEnd: 0,
  packets: [],
  lastSentCompressStart: 0,
  lastSentCompressEnd: 0,
};

let packetsReceivedInCurrentSecond = 0;

function ConnectSocket(mode, ip, port) {
  try {
    timingSummary.connectStart = performance.now();

    const url = `${mode}://${ip}:${port}`;
    objSocket = new WebSocket(url);
    objSocket.binaryType = 'arraybuffer';

    objSocket.on('open', () => {
      timingSummary.connectEnd = performance.now();
      console.log('Socket connected!');
      const sLoginRequest =
        '63=FT3.0|64=101|65=74|66=14:59:22|67=TEST|68=YOUR_API_KEY|4=|400=0|401=2|396=HO|51=4|395=127.0.0.1';
      SendMessageOnSocket(sLoginRequest);
    });

    objSocket.on('close', () => {
      console.log('Socket closed');
      printTimingSummary();
    });

    objSocket.on('error', (evt) => {
      console.error('Socket error:', evt);
    });

    objSocket.on('message', socketOnMessage);

    console.log(`Connecting to ${url} ...`);
  } catch (ex) {
    console.error('ConnectSocket error:', ex);
  }
}

async function socketOnMessage(data) {
  const receiveStart = performance.now();
  try {
    const bufferData = data instanceof Buffer ? data : Buffer.from(data);

    let dataReceived = baOldData ? Buffer.concat([baOldData, bufferData]) : bufferData;
    baOldData = null;

    const intRawPktLen = dataReceived.length;
    let i = 0;
    let isBroken = false;
    let totalPacketLength = 0;
    const dataPacketLengthList = [];

    if (intRawPktLen > 5) {
      while (i < intRawPktLen) {
        if (dataReceived[i] === 5 || dataReceived[i] === 2) {
          const strPacketLength = dataReceived.slice(i + 1, i + 6).toString('ascii');
          if (strPacketLength.length === 5) {
            const packetLength = parseInt(strPacketLength, 10);
            dataPacketLengthList.push(packetLength + 6);
            totalPacketLength += packetLength + 6;
            i += 6 + packetLength;
          } else {
            baOldData = dataReceived.slice(i);
            isBroken = true;
            break;
          }
        } else {
          i++;
        }
      }
    } else {
      baOldData = dataReceived;
    }

    if (intRawPktLen === totalPacketLength) {
      let j = 0;
      for (let k of dataPacketLengthList) {
        const uncompData = dataReceived.slice(j, k);
        const decompressStart = performance.now();
        await ProcessSocketMessage(uncompData);
        const decompressEnd = performance.now();
        timingSummary.packets.push({
          receiveStart,
          receiveEnd: performance.now(),
          decompressStart,
          decompressEnd,
        });
        j = k;
      }
      baOldData = null;
    } else {
      let j = 0,
        k = dataPacketLengthList.length > 0 ? dataPacketLengthList[0] : 0;
      if (!isBroken) {
        for (let idx = 0; idx < dataPacketLengthList.length - 1; idx++) {
          const uncompData = dataReceived.slice(j, k);
          await ProcessSocketMessage(uncompData);
          j = k;
          k += dataPacketLengthList[idx + 1];
        }
        if (dataPacketLengthList.length > 0) {
          baOldData = dataReceived.slice(j, k);
        }
      } else {
        for (let idx = 0; idx < dataPacketLengthList.length; idx++) {
          const uncompData = dataReceived.slice(j, k);
          await ProcessSocketMessage(uncompData);
          j = k;
          k += dataPacketLengthList[idx + 1];
        }
      }
    }
  } catch (ex) {
    console.error('socketOnMessage error:', ex);
  }
}

async function ProcessSocketMessage(uncompData) {
  const _response = DeCompressData(uncompData);

  if (!_response) {
    return;
  }

  const intTmtrIndex = _response.indexOf('\u0000');
  let response = intTmtrIndex !== -1 ? _response.substring(0, intTmtrIndex) : _response;

  const arrData = response.split('\u0002');
  for (const packet of arrData) {
    if (packet !== '') {
      packetsReceivedInCurrentSecond++;
      try {
        const match1 = packet.match(/\|1=([^\|]+)/);
        const match7 = packet.match(/\|7=([^\|]+)/);

        if (!match1 || !match7) {
          continue;
        }

        const y = match1[1];
        const x = match7[1];
        const redisKey = `${y}_${x}`;

      const cleanedPacket = packet.replace(/^\d{7}=FIX3\.0\|/, '');

        const redisStart = performance.now();
        try {
          const lastEntry = await redisClient.lIndex(redisKey, -1);
          if (lastEntry !== cleanedPacket) {
            await redisClient.rPush(redisKey, cleanedPacket);
            console.log(`Stored packet in Redis [${redisKey}]: ${cleanedPacket}`);
          } else {
            console.log(`Duplicate packet ignored for Redis key [${redisKey}]`);
          }
        } catch (redisErr) {
          console.error('Error saving packet to Redis:', redisErr);
        }
        const redisEnd = performance.now();

        const pkt = timingSummary.packets[timingSummary.packets.length - 1];
        pkt.redisStart = redisStart;
        pkt.redisEnd = redisEnd;

      } catch (redisErr) {
        console.error('Error saving packet to Redis:', redisErr);
      }
    }
  }
}

function DeCompressData(_data) {
  try {
    const compressedData = _data.slice(6);
    const decompressedData = Zlib.uncompress(new Uint8Array(compressedData));
    return String.fromCharCode(...decompressedData);
  } catch (e) {
    console.error('Decompression error:', e);
    return '';
  }
}

function SendMessageOnSocket(msg) {
  try {
    if (objSocket && objSocket.readyState === WebSocket.OPEN) {
      const fragmented = fragmentData(msg);
      objSocket.send(fragmented);
      console.log(`Sent message: ${msg}`);
    }
  } catch (ex) {
    console.error('SendMessageOnSocket error:', ex);
  }
}

function fragmentData(_requestPacket) {
  try {
    const _strHead = String.fromCharCode(5);
    const _headerBytes = Buffer.from(_strHead, 'ascii');
    const _baRequest = HandleCompressedData(_requestPacket);
    const _length = _baRequest.length + 4;
    const _lengthString = _length.toString().padStart(5, '0');
    const _lenBytes = Buffer.from(_lengthString, 'ascii');
    const _baActualSend = Buffer.alloc(5 + _length);
    _lenBytes.copy(_baActualSend, 0);
    _baRequest.copy(_baActualSend, 5);
    return Buffer.concat([_headerBytes, _baActualSend]);
  } catch (e) {
    console.error('fragmentData error:', e);
  }
}

function HandleCompressedData(_rawData) {
  try {
    const start = performance.now();
    const inputBuffer = Buffer.from(_rawData, 'utf8');
    const compressed = Zlib.compress(new Uint8Array(inputBuffer), 6);
    const end = performance.now();
    timingSummary.lastSentCompressStart = start;
    timingSummary.lastSentCompressEnd = end;
    return Buffer.from(compressed);
  } catch (e) {
    console.error('Compression error:', e);
  }
}

function printTimingSummary() {
  const connectDuration = timingSummary.connectEnd - timingSummary.connectStart;
  console.log(`Connection took: ${connectDuration.toFixed(2)} ms`);

  timingSummary.packets.forEach((pkt, i) => {
    const receiveDuration = pkt.receiveEnd - pkt.receiveStart;
    const decompressDuration = pkt.decompressEnd - pkt.decompressStart;
    const redisDuration = pkt.redisEnd && pkt.redisStart ? (pkt.redisEnd - pkt.redisStart) : 0;
    console.log(
      `Packet ${i + 1}: Receive = ${receiveDuration.toFixed(2)} ms, ` +
      `Decompress = ${decompressDuration.toFixed(2)} ms, ` +
      `Redis = ${redisDuration.toFixed(2)} ms`
    );
  });

  if (timingSummary.lastSentCompressEnd && timingSummary.lastSentCompressStart) {
    const compressDuration = timingSummary.lastSentCompressEnd - timingSummary.lastSentCompressStart;
    console.log(`Last compression took: ${compressDuration.toFixed(2)} ms`);
  }
}

async function logRedisMemoryUsage() {
  try {
    const keys = await redisClient.keys('*_*');
    if (keys.length === 0) {
      console.log('No keys found in Redis.');
      return;
    }

    const usages = await Promise.all(
      keys.map(async (key) => await redisClient.memoryUsage(key) || 0)
    );

    const totalBytes = usages.reduce((acc, val) => acc + val, 0);
    console.log(`Redis memory used by ${keys.length} keys: ${totalBytes} bytes (${(totalBytes / (1024 * 1024)).toFixed(3)} MB)`);
  } catch (e) {
    console.error('Error checking Redis memory usage:', e);
  }
}

// Start
ConnectSocket('wss', 'wave.bpwealth.com', 4511);

setTimeout(() => {
  let sMsg = '';
  try {
    sMsg = fs.readFileSync('underlyingconfig.txt', 'utf8').trim();
  } catch (err) {
    console.error('Error reading config.txt:', err);
    return;
  }

  const sTokenArr = sMsg.split(',');
  let sMsgToSend = '63=FT3.0|64=206|65=84|66=19:02:31|4=|';

  for (const token of sTokenArr) {
    const parts = token.split('_');
    sMsgToSend += `1=${parts[0]}$7=${parts[1]}|`;
  }
  sMsgToSend += '230=1';

  SendMessageOnSocket(sMsgToSend);
}, 3000);




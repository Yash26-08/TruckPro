const fs = require('fs');
const csv = require('csv-parser');
const mqtt = require('mqtt');
const path = require('path');

// === MQTT Configuration ===
const ENDPOINT = 'a1ef8l1w2rzfkw-ats.iot.us-east-1.amazonaws.com';
const PORT = 8883;
const TOPIC = 'truck/telemetry';
const FILES = ['truck_data_upload.csv']; // You can change or extend this list

const options = {
  host: ENDPOINT,
  port: PORT,
  protocol: 'mqtts',
  cert: fs.readFileSync(path.join(__dirname, 'certificate.pem.crt')),
  key: fs.readFileSync(path.join(__dirname, 'private.pem.key')),
  ca: fs.readFileSync(path.join(__dirname, 'AmazonRootCA1.pem')),
  rejectUnauthorized: true,
  clientId: 'TruckUploader_' + Math.random().toString(16).slice(2, 10),
};

// === Create MQTT Client ===
const client = mqtt.connect(options);

client.on('connect', async () => {
  console.log('✅ Connected to AWS IoT Core\n');

  try {
    for (const file of FILES) {
      const filePath = path.join(__dirname, file);
      const truckId = path.basename(file, '.csv').toUpperCase();

      console.log(`🚛 Publishing data from: ${file}`);

      await new Promise((resolve) => {
        fs.createReadStream(filePath)
          .pipe(csv())
          .on('data', (row) => {
            try {
              const payload = {
                ...row,
                truck_id: row.truck_id || truckId,
                distance_covered_km: parseFloat(row.distance_covered_km),
                load_tonnes: parseFloat(row.load_tonnes),
                base_rate: parseFloat(row.base_rate),
                rate_per_km: parseFloat(row.rate_per_km),
                engine_temp_c: parseFloat(row.engine_temp_c),
                oil_level_percent: parseFloat(row.oil_level_percent),
                coolant_level_percent: parseFloat(row.coolant_level_percent),
                battery_voltage_v: parseFloat(row.battery_voltage_v),
                brake_pad_health: parseFloat(row.brake_pad_health),
                timestamp: row.timestamp,
              };

              const message = JSON.stringify(payload);
              client.publish(TOPIC, message);
              console.log(`   📤 Published row at ${payload.timestamp} from ${payload.truck_id}`);
            } catch (publishErr) {
              console.error(`   ❌ Error publishing row:`, publishErr.message);
            }
          })
          .on('end', () => {
            console.log(`✅ Completed: ${file}\n`);
            setTimeout(resolve, 2000); // Delay to avoid message burst
          })
          .on('error', (err) => {
            console.error(`❌ File read error for ${file}:`, err.message);
            resolve(); // Continue to next file
          });
      });
    }

    console.log('🚀 All files published. Disconnecting...');
    client.end();
  } catch (err) {
    console.error('❌ Unexpected error during publishing:', err.message);
    client.end();
  }
});

client.on('error', (err) => {
  console.error('❌ MQTT connection error:', err.message);
});

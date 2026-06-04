import http from 'k6/http';
import { check } from 'k6';

// Inside chaos.js
export const options = {
  stages: [
    { duration: '2m', target: 300 }, // Gentle ramp to 300 users to let KEDA wake up
    { duration: '5m', target: 300 }, // The Crucible: 5 unbroken minutes of maximum sustained load
    { duration: '1m', target: 0 },   // Graceful scale-down
  ],
};

// 1. SETUP: Runs exactly once before the test starts
export function setup() {
    const startTime = new Date();
    console.log(`\n🚀 INITIATING SUSTAINED CHAOS...`);
    console.log(`🕒 Start Time: ${startTime.toLocaleString()}\n`);
    
    // Pass the start time down to the teardown function
    return { start: startTime.getTime() };
}

// 2. DEFAULT: Runs continuously for every Virtual User
export default function () {
    const url = 'https://api-router-eastus.happytree-0d70414b.eastus.azurecontainerapps.io/transaction';
    
    const payload = JSON.stringify({
        accountId: `load_user_${__VU % 10}`, 
        amount: 10.00,
        currency: 'USD'
    });

    const params = {
        headers: {
            'Content-Type': 'application/json',
        },
    };

    const res = http.post(url, payload, params);

    check(res, {
        'transaction successful (200)': (r) => r.status === 200,
    });
}

// 3. TEARDOWN: Runs exactly once after all VUs finish
export function teardown(data) {
    const endTime = new Date();
    const durationMs = endTime.getTime() - data.start;
    const durationSec = (durationMs / 1000).toFixed(2);

    console.log(`\n=====================================`);
    console.log(`✅ SUSTAINED TEST COMPLETE!`);
    console.log(`🕒 End Time: ${endTime.toLocaleString()}`);
    console.log(`⏱️ Total Duration: ${durationSec} seconds`);
    console.log(`=====================================\n`);
}
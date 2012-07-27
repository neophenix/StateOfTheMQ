#!/usr/bin/env node

var redis = require('redis'),
    client = redis.createClient("6379", "");

var low = 50000,
    high = 0,
    run = 0,
    total_m = 0,
    total_s = 0,
    avg = 0,
    msgs = 0,
    per_sec = 0;

setInterval(function() {
    if ( msgs > 0 ) {
        if ( run > 1 && msgs > high ) high = msgs;
        if ( run > 1 && msgs < low ) low = msgs;
        total_m += msgs;
        total_s++;
        avg = total_m / total_s;
        run++;

        console.log("Totals ["+low+"] ["+high+"] ["+avg+"]");
        msgs = 0;
    }
}, 1000);

client.on("error", function (err) {
    console.log("Error " + err);
});
client.on("message", function (channel, message) {
    msgs++;
});
client.on("ready", function () {
    client.subscribe("1");
});

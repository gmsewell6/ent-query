'use strict';

const stream = require('stream');

class RedisReadStream extends stream.Readable {
    constructor(client, key) {
        super({ objectMode: true });
        this.client = client;
        this.key = key;
        this.index = 0;
    }

    _read(done) {
        this.client.hget(this.key, ['record', this.index++].join(':'), function (err, result) {
            if (err) return this.emit('error', err);

            this.push(result ? JSON.parse(result) : null);
            done();
        });
    }
}
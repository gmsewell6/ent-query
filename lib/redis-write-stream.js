'use strict';

const stream = require('stream');

class RedisWriteStream extends stream.Transform {
    constructor(client, key) {
        super({ objectMode: true });
        this.client = client;
        this.key = key;
        this.count = 0;
    }
    _transform(record, enc, done) {
        this.client.multi()
            .hset(this.key, ['record', this.count++].join(':'), JSON.toString(record))
            .hset(this.key, 'count', this.count)
            .exec(err => done(err, record));
    }
}

exports.RedisWriteStream = RedisWriteStream;
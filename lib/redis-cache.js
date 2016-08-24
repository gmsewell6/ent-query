'use strict';

const P = require('bluebird');
const stream = require('stream');

class RedisReadStream extends stream.Readable {
    constructor(client, key) {
        super({ objectMode: true });
        this.client = client;
        this.key = key;
        this.index = 0;
    }

    _read() {
        var self = this;
        this.client.hget(this.key, ['record', this.index++].join(':'), function (err, result) {
            if (err) return this.emit('error', err);

            self.push(result ? JSON.parse(result) : null);
        });
    }
}

class RedisWriteStream extends stream.Transform {
    constructor(client, key) {
        super({ objectMode: true });
        this.client = client;
        this.key = key;
        this.count = 0;
    }
    _transform(record, enc, done) {
        this.client.multi()
            .hset(this.key, ['record', this.count++].join(':'), JSON.stringify(record))
            .hset(this.key, 'count', this.count)
            .exec(err => done(err, record));
    }
}

class RedisCache {
    constructor(client, duration) {
        this.client = client;
        this.duration = duration || 60 * 60; // one hour
    }

    exists(key) {
        var self = this;

        return new P(function (resolve, reject) {
            self.client.exists(key, function (err, exists) {
                if (err) return reject(err);

                resolve(!!exists);
            });
        });
    }

    read(key, reply) {
        const self = this;

        return new P(function (resolve, reject) {
            self.client.multi()
                .exists(key)
                .hget(key, 'selected')
                .hget(key, 'fields')
                .exec(function (err, replies) {
                    // error
                    if (err) return reject(err);

                    // doesnt exist
                    if (!replies[0]) return resolve();

                    reply(new RedisReadStream(self.client, key))
                        .selected(replies[1])
                        .fields(JSON.parse(replies[2]));
                });
        });
    }

    write(key, result) {
        var self = this;
        result.through(s => s.pipe(new RedisWriteStream(this.client, key)));

        return new P(function (resolve, reject) {
            self.client.multi()
                .hset(key, 'selected', parseInt(result.selected))
                .hset(key, 'fields', JSON.stringify(result.fields))
                .expire(key, self.duration)
                .exec(err => {
                    if (err) return reject(err);
                    resolve();
                });
        });
    }
}

exports.RedisCache = RedisCache;
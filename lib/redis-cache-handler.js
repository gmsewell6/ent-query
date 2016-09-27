'use strict';

const P = require('bluebird');
const stream = require('stream');
const joi = require('joi');
const redis = require('redis');
const hash = require('object-hash');
const Logger = require('glib').Logger;
const log = new Logger(['query-cache']);
const _ = require('lodash');

const optionsSchema = {
    client: joi.object().default(() => redis.createClient(), 'default factory'),
    expires: joi.number().default(60 * 60),
    hash: joi.func().default(q => hash(_.pick(q, 'limit', 'language', 'user', 'payload'))),
    cacheKey: joi.func().default(query => hash(query))
};

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

function cachedHandler(handler, options) {
    options = joi.attempt(options || {}, optionsSchema);

    return function (query, reply) {
        const client = options.client;
        const key = options.cacheKey(query);
        const signature = options.hash(query);

        const cacheResult = function (result) {
            return new P(function (resolve) {
                // put serializer at the front of the line
                result.throughHandlers.unshift(s => s.pipe(new RedisWriteStream(client, key)));
                const cmd = client.multi()
                    .hset(key, 'selected', parseInt(result.selected))
                    .hset(key, 'fields', JSON.stringify(result.fields))
                    .hset(key, 'hash', signature);

                if (options.expires > 0) cmd.expire(key, options.expires);

                cmd.exec(err => {
                    if (err) log.error('Error writing to query cache', err);

                    resolve();
                });
            });
        };

        const invokeHandler = function () {
            // attach result listener to cache metadata and attach
            query.post(cacheResult);
            handler(query, reply);
        };

        client.multi()
            .exists(key)
            .hget(key, 'selected')
            .hget(key, 'fields')
            .hget(key, 'hash')
            .exec(function (err, replies) {
                // error
                if (err) {
                    log.error('Error reading from cache, invoking handler...', err);
                    return invokeHandler(query, reply);
                }

                // doesnt exist
                if (!replies[0] || signature !== replies[3]) return invokeHandler(query, reply);

                reply(new RedisReadStream(client, key))
                    .selected(replies[1])
                    .fields(JSON.parse(replies[2]));
            });
    };
}

module.exports = cachedHandler;
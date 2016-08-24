'use strict';

const _ = require('lodash');
const Query = require('../lib/query2').Query;
const QueryResult = require('../lib/query2').QueryResult;
const RedisCache = require('../lib/redis-cache').RedisCache;
const redis = require('redis');
const client = redis.createClient();

const chai = require('chai');
const should = chai.should();
const sinon = require('sinon');
const P = require('bluebird');
P.promisifyAll(redis.RedisClient.prototype);
P.promisifyAll(redis.Multi.prototype);

const people = [{ first: 'Brad', last: 'Leupen' }, { first: 'Hank', last: 'Leupen' }];

chai.use(require('sinon-chai'));
chai.use(require('chai-as-promised'));

describe.only('Query2', function () {
    describe('handler()', function () {
        it('should return the query', function () {
            const q = new Query('select * from accounts');
            q.should.equal(q.handler(_.noop));
        });

        it('should set the handler', function () {
            const q = new Query('select * from accounts').handler(_.noop);
            should.exist(q._handler);
        });
    });

    describe('execute()', function () {
        it('should return a promise', function () {
            new Query('select * from accounts')
                .handler(_.noop)
                .execute()
                .should.respondTo('then');
        });

        it('should invoke the handler function', function () {
            const handler = sinon.spy(function(query, reply) {
                reply();
            });

            return new Query('select * from accounts')
                .handler(handler)
                .execute()
                .then(() => handler.should.have.been.called);
        });

        it('should pass in the query context', function () {
            const handler = function (query, reply) {
                query.should.have.property('payload', 'select * from accounts where state = :state');
                query.should.have.property('params').that.deep.equals({ state: 'NC' });
                reply();
            };

            return new Query('select * from accounts where state = :state')
                .handler(handler)
                .execute({ state: 'NC' });
        });

        it('should return a shim from the reply function', function () {
            const handler = function (query, reply) {
                var shim = reply();
                shim.should.respondTo('field');
            };

            return new Query('select * from accounts where state = :state')
                .handler(handler)
                .execute();
        });

        it('should resolve to a query result object', function () {
            const handler = function (query, reply) {
                var shim = reply();
                shim.should.respondTo('field');
            };

            return new Query('select * from accounts where state = :state')
                .handler(handler)
                .execute()
                .then(qr => qr.should.be.an.instanceOf(QueryResult));
        });

        it('should emit an execute event', function () {
            const handler = function (query, reply) {
                reply([]);
            };

            const spy = sinon.spy();

            return new Query('select * from accounts')
                .handler(handler)
                .on('execute', spy)
                .execute()
                .then(() => spy.should.have.been.called);
        });

        it('should emit a result event', function () {
            const handler = function (query, reply) {
                reply([]);
            };

            const spy = sinon.spy();

            return new Query('select * from accounts')
                .handler(handler)
                .on('result', spy)
                .execute()
                .then(r => spy.should.have.been.calledWith(r));
        });
    });

    describe('QueryResult.stream()', function () {
        it('should stream the data', function () {
            const handler = function(query, reply) {
                reply(people).fields(['firstName', 'lastName']);
            };

            return new Query('select * from person')
                .handler(handler)
                .execute()
                .then(qr => qr.stream())
                .then(s => new P(r => s.toArray(r)))
                .then(d => d.should.deep.equal(people));
        });
    });

    describe('QueryResult.toArray()', function () {
        it('should return a promise to a buffered array', function () {
            const handler = function(query, reply) {
                reply(people).fields(['firstName', 'lastName']);
            };

            return new Query('select * from person')
                .handler(handler)
                .execute()
                .then(qr => qr.toArray())
                .then(d => d.should.deep.equal(people));
        });
    });

    describe('cacheKey()', function () {
        it('should set the cache key', function () {
            const q = new Query('select * from accounts');
            q.should.equal(q.cacheKey('data'));
            q._cacheKey.should.equal('data');
        });
    });

    describe('cache', function () {
        beforeEach(function () {
            return client.delAsync('data');
        });

        it('should set the cache interface', function () {
            const q = new Query('select * from accounts');
            const cache = new RedisCache(client);

            q.should.equal(q.cache(cache));
            q._cache.should.equal(cache);
        });

        it('should write the selected field to redis', function () {
            const handler = function(query, reply) {
                reply(people)
                    .fields(['firstName', 'lastName'])
                    .selected(2);
            };

            return new Query('select * from accounts')
                .cache(new RedisCache(client))
                .cacheKey('data')
                .handler(handler)
                .execute()
                .then(() => client.hgetAsync('data', 'selected'))
                .should.eventually.equal('2');
        });

        it('should write the data to redis', function () {
            const handler = function(query, reply) {
                reply(people)
                    .fields(['firstName', 'lastName'])
                    .selected(2);
            };

            return new Query('select * from accounts')
                .cache(new RedisCache(client))
                .cacheKey('data')
                .handler(handler)
                .execute()
                .then(qr => qr.toArray())
                .then(() => client.hgetAsync('data', 'record:0'))
                .should.eventually.equal(JSON.stringify({ first: 'Brad', last: 'Leupen' }))
                .then(() => client.hgetAsync('data', 'record:1'))
                .should.eventually.equal(JSON.stringify({ first: 'Hank', last: 'Leupen' }));
        });
    });
});
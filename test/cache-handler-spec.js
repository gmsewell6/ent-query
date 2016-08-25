'use strict';

const _ = require('lodash');
const query = require('../lib/query2');
const redis = require('redis');
const client = redis.createClient();

const chai = require('chai');
const should = chai.should();
const sinon = require('sinon');
const P = require('bluebird');
P.promisifyAll(redis.RedisClient.prototype);
P.promisifyAll(redis.Multi.prototype);

const people = [{ first: 'Brad', last: 'Leupen' }, { first: 'Hank', last: 'Leupen' }];

describe('cachedHandler', function () {
    beforeEach(function () {
        return client.delAsync('data');
    });

    it('should write the selected field to redis', function () {
        const handler = function(query, reply) {
            reply(people)
                .fields(['firstName', 'lastName'])
                .selected(2);
        };

        return query.create('select * from accounts')
            .handler(query.cachedHandler(handler, { cacheKey: _.constant('data') }))
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

        return query.create('select * from accounts')
            .handler(query.cachedHandler(handler, { cacheKey: _.constant('data') }))
            .execute()
            .then(qr => qr.toArray())
            .then(() => client.hgetAsync('data', 'record:0'))
            .should.eventually.equal(JSON.stringify({ first: 'Brad', last: 'Leupen' }))
            .then(() => client.hgetAsync('data', 'record:1'))
            .should.eventually.equal(JSON.stringify({ first: 'Hank', last: 'Leupen' }));
    });

    describe('when a query has been cached', function () {
        const handler = function(query, reply) {
            reply(people)
                .fields(['firstName', 'lastName'])
                .selected(2);
        };

        beforeEach(function () {
            return query.create('select * from accounts')
                .handler(query.cachedHandler(handler, { cacheKey: _.constant('data') }))
                .toArray();
        });

        it('should not invoke the handler again', function () {
            const handler = sinon.spy();

            return query.create('select * from accounts')
                .handler(query.cachedHandler(handler, { cacheKey: _.constant('data') }))
                .toArray()
                .then(function (arr) {
                    arr.should.have.deep.members(people);
                    handler.should.not.have.been.called;
                });
        });
    });
});
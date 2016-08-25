'use strict';

const _ = require('lodash');
const query = require('../lib/query2');
const Query = query.Query;
const QueryResult = query.QueryResult;

const chai = require('chai');
const should = chai.should();
const sinon = require('sinon');
const P = require('bluebird');

const people = [{ first: 'Brad', last: 'Leupen' }, { first: 'Hank', last: 'Leupen' }];

chai.use(require('sinon-chai'));
chai.use(require('chai-as-promised'));

describe('Query2', function () {
    describe('handler()', function () {
        it('should return the query', function () {
            const q = query.create('select * from accounts');
            q.should.equal(q.handler(_.noop));
        });

        it('should set the handler', function () {
            const q = query.create('select * from accounts').handler(_.noop);
            should.exist(q.defn._handler);
        });
    });

    describe('execute()', function () {
        it('should return a promise', function () {
            query.create('select * from accounts')
                .handler(_.noop)
                .execute()
                .should.respondTo('then');
        });

        it('should invoke the handler function', function () {
            const handler = sinon.spy(function(query, reply) {
                reply();
            });

            return query.create('select * from accounts')
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

            return query.create('select * from accounts where state = :state')
                .handler(handler)
                .execute({ state: 'NC' });
        });

        it('should return a shim from the reply function', function () {
            const handler = function (query, reply) {
                var shim = reply();
                shim.should.respondTo('field');
            };

            return query.create('select * from accounts where state = :state')
                .handler(handler)
                .execute();
        });

        it('should resolve to a query result object', function () {
            const handler = function (query, reply) {
                var shim = reply();
                shim.should.respondTo('field');
            };

            return query.create('select * from accounts where state = :state')
                .handler(handler)
                .execute()
                .then(qr => qr.should.be.an.instanceOf(QueryResult));
        });

        it('should emit an execute event', function () {
            const handler = function (query, reply) {
                reply([]);
            };

            const spy = sinon.spy();

            return query.create('select * from accounts')
                .handler(handler)
                .build()
                .on('execute', spy)
                .execute()
                .then(() => spy.should.have.been.called);
        });

        it('should emit a result event', function () {
            const handler = function (query, reply) {
                reply([]);
            };

            const spy = sinon.spy();

            return query.create('select * from accounts')
                .handler(handler)
                .build()
                .on('result', spy)
                .execute()
                .then(r => spy.should.have.been.calledWith(r));
        });

        it('should include a post() function on the context object', function () {
            const handler = function(query, reply) {
                query.should.respondTo('post');
                reply();
            };

            return query.create('select * from accounts')
                .handler(handler)
                .execute();
        });

        it('should invoke the post function before returning the result', function () {
            const spy = sinon.spy();

            const handler = function(query, reply) {
                query.post(spy);
                reply();
            };

            return query.create('select * from accounts')
                .handler(handler)
                .execute()
                .then(r => spy.should.have.been.called);
        });
    });

    describe('QueryResult.stream()', function () {
        it('should stream the data', function () {
            const handler = function(query, reply) {
                reply(people).fields(['firstName', 'lastName']);
            };

            return query.create('select * from person')
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

            return query.create('select * from person')
                .handler(handler)
                .execute()
                .then(qr => qr.toArray())
                .then(d => d.should.deep.equal(people));
        });
    });
});
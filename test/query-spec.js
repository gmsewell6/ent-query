'use strict';

var sinon = require('sinon');
var chai = require('chai');
var P = require('bluebird');
chai.use(require('chai-as-promised'));
chai.use(require('sinon-chai'));
var should = chai.should();

var Query = require('../lib/query').Query;
var QueryResult = require('../lib/query').QueryResult;

describe('Query', function () {
    describe('execute()', function () {
        describe('when no handler is defined', function () {
            it('should reject the promise', function () {
                return new Query('select * from country').execute().should.eventually.be.rejectedWith('Handler method not implemented. Did you forget to call handler()?');
            });
        });

        describe('when a handler is defined', function () {
            it('should invoke the handler', function () {
                var spy = sinon.spy(function (query, reply) {
                    query.payload.should.equal('select * from country');
                    reply();
                });

                var query = new Query('select * from country')
                    .handler(spy);

                return query
                    .execute({ foo: 'bar' })
                    .then(function () {
                        spy.should.have.been.calledWith(query);
                    });
            });

            it('should resolve with a query result object', function () {
                return new Query('select * from country')
                    .handler(function (query, reply) {
                        reply(null, [{ first: 'Brad', last: 'Leupen' }]);
                    })
                    .execute({ foo: 'bar' })
                    .then(function (result) {
                        result.should.be.an.instanceOf(QueryResult);
                    });
            });

            it('should emit a cancellation event on the query', function () {
                var spy = sinon.spy();

                var handler = function(query, reply) {
                    query.on('cancel', spy)
                };

                var query = new Query().handler(handler);

                query.execute();
                query.cancel();

                spy.should.have.been.called;
            });

            it('should reject the promise with an asynchronous error', function () {
                return new Query()
                    .handler(function (query, reply) {
                        setImmediate(function() {
                            reply(new Error('failed'));
                        })
                    })
                    .execute()
                    .should.eventually.be.rejectedWith('failed');
            });

            it('should reject the promise with a synchronous error', function () {
                return new Query()
                    .handler(function () {
                        throw new Error('failed');
                    })
                    .execute()
                    .should.eventually.be.rejectedWith('failed');
            });
        });
    });

    describe('stream()', function () {
        it('should return a stream that eventually resolves', function () {
            var handler = function(query, reply) {
                reply(null, [{ first: 'Brad', last: 'Leupen' }, { first: 'Hank', last: 'Leupen' }]);
            };

            var stream = new Query('select * from country')
                .handler(handler)
                .stream();

            should.exist(stream);

            return new P(function (resolve) {
                stream.toArray(resolve);
            })
                .then(function (data) {
                    data.should.have.length(2);
                    data.should.have.deep.members([{ first: 'Brad', last: 'Leupen' }, { first: 'Hank', last: 'Leupen' }]);
                });
        });
    });

    describe('toArray()', function () {
        it('should return the results array', function () {
            return new Query()
                .handler(function (query, reply) {
                    reply(null, ['foo', 'bar', 'baz']);
                })
                .toArray()
                .then(function(arr) {
                    arr.should.have.length(3);
                    arr.should.have.members(['foo', 'bar', 'baz']);
                })
        });
    });
});

describe('QueryResult', function () {
    describe('cancel()', function () {
        it('should forward cancellations to the parent query', function () {
            var spy = sinon.spy();
            var query = new Query();
            var result = new QueryResult(query);
            query.on('cancel', spy);
            result.cancel();
            spy.should.have.been.called;
        });
    });

    describe('toArray()', function () {
        it('should return a promise to an array of the result stream', function () {
            var query = new Query();
            var qr = new QueryResult(query, [{ first: 'Brad' }, { first: 'Hank' }]);
            return qr.toArray()
                .then(function(arr) {
                    arr.should.have.length(2);
                    arr.should.have.deep.members([{ first: 'Brad' }, { first: 'Hank' }]);
                })
        });
    });

    describe('shim()', function () {
        var qr, shim;

        beforeEach(function () {
            qr = new QueryResult(new Query());
            shim = qr.shim();
        });

        it('should set the results fields', function () {
            shim.should.respondTo('fields');
            shim.fields(['first', 'last']);

            qr.fields.should.deep.equal(['first', 'last']);
        });

        it('should set the results meta', function () {
            shim.should.respondTo('meta');
            shim.meta('whatever');

            qr.meta.should.equal('whatever');
        });

        it('should set the selected number of rows', function () {
            shim.should.respondTo('selected');
            shim.selected(100);

            qr.selected.should.equal(100);
        });
    });
});
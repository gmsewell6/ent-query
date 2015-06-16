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
    describe('handler()', function () {
        it('should bind to a receiver', function () {
            var foo = {
                handler: function (query, reply) {
                    foo.should.equal(this);
                    reply();
                }
            };

            new Query().handler(foo.handler, foo).execute();
        });
    });

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
                    query.params.should.deep.equal({ foo: 'bar' });
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

                var query = new Query()
                    .on('cancel', spy)
                    .cancel();

                spy.should.have.been.called;
                query.cancelled.should.be.true;
            });

            it('should not be invoked if query is cancelled before execution', function () {
                var spy = sinon.spy(function(query, reply) {
                    reply();
                });

                new Query()
                    .handler(spy)
                    .cancel()
                    .execute()
                    .should.eventually.be.rejectedWith('cancellation error')
                    .then(function () {
                        spy.should.not.have.been.called;
                    });
            });

            it('should emit a cancellation event when the execution promise is cancelled', function () {
                var spy = sinon.spy();

                var handler = function (query, reply) {
                    reply();
                };

                var q = new Query()
                    .handler(handler)
                    .on('cancel', spy);

                return q.execute()
                    .cancel()
                    .should.eventually.be.rejectedWith('cancellation error')
                    .then(function () {
                        spy.should.have.been.called;
                        q.cancelled.should.be.true;
                    });
            });

            it('should pass along first reply value if not an error', function () {
                return new Query()
                    .handler(function (query, reply) {
                        reply([{ first: 'brad', last: 'leupen' }]);
                    })
                    .execute()
                    .then(function (result) {
                        return result.toArray();
                    })
                    .then(function (arr) {
                        arr.should.have.deep.members([{ first: 'brad', last: 'leupen' }]);
                    });
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

        it('should return the same promise when executed twice', function () {
            var query = new Query()
                .handler(function(query, reply) {
                    reply();
                });

            query.execute().should.equal(query.execute());
        });
    });

    describe('through()', function () {
        it('should support adding a through function', function () {
            var query = new Query();
            query.should.respondTo('through');

            query.through(function () {}).should.equal(query);
            query.should.have.property('_through').that.has.length(1);
        });

        it('should invoke the through function when the query is streamed', function () {
            var spy = sinon.spy(function (stream) {
                return stream.tap(function(r) {
                    r.full = r.first + ' ' + r.last;
                });
            });

            return new Query()
                .handler(function (query, reply) {
                    reply(null, [{ first: 'Brad', last: 'Leupen' }]);
                })
                .through(spy)
                .execute()
                .then(function (result) {
                    return result.toArray()
                })
                .then(function (arr) {
                    arr.should.have.length(1);
                    arr.should.have.deep.members([{ first: 'Brad', last: 'Leupen', full: 'Brad Leupen' }]);
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

    describe('use()', function () {
        it('should invoke the middleware function', function () {
            var mw = sinon.spy();
            var query = new Query();
            query.use(mw).should.equal(query);
            mw.should.have.been.calledWith(query);
        });
    });

    describe('post()', function () {
        it('should intercept the query result before the stream starts', function () {
            var spy = sinon.spy(function(stream) {
                return stream.tap(function (r) {
                    r.full = r.first + ' ' + r.last;
                });
            });

            return new Query()
                .handler(function (query, reply) {
                    reply(null, [{ first: 'Brad', last: 'Leupen' }]).fields(['first', 'last']);
                })
                .through(spy)
                .post(function (result) {
                    result.fields.should.have.members(['first', 'last']);
                    result.fields.push('full');
                    spy.should.not.have.been.called;
                })
                .execute()
                .then(function (result) {
                    result.should.be.an.instanceOf(QueryResult);
                    result.fields.should.have.members(['first', 'last', 'full']);
                    return result.toArray();
                })
                .then(function (arr) {
                    arr.should.have.length(1);
                    arr.should.have.deep.members([{ first: 'Brad', last: 'Leupen', full: 'Brad Leupen' }]);
                });
        });
    });

    describe('pre()', function () {
        it('should intercept the query before the handler is called', function () {
            var handlerSpy = sinon.spy(function(query, reply) {
                reply();
            });

            var preSpy = sinon.spy(function (query) {
                query.should.be.an.instanceOf(Query);
                query.should.equal(q);
                handlerSpy.should.not.have.been.called;
            });

            var q = new Query()
                .handler(handlerSpy)
                .pre(preSpy);

            return q.execute()
                .then(function() {
                    preSpy.should.have.been.called;
                    handlerSpy.should.have.been.called;
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
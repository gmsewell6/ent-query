'use strict';

const _ = require('lodash');
const query = require('../lib');

const $ = require('highland');
const chai = require('chai');
const should = chai.should();
const sinon = require('sinon');
const P = require('bluebird');
const Query = require('../lib/query').Query;
const QueryResult = require('../lib/query-result').QueryResult;
const FieldConfigurator = require('../lib/field-configurator').FieldConfigurator;
const people = [{ first: 'Brad', last: 'Leupen' }, { first: 'Hank', last: 'Leupen' }];
const { Readable, Writable, pipeline } = require('stream');

chai.use(require('sinon-chai'));
chai.use(require('chai-as-promised'));

describe('QueryBuilder', function () {
    describe('handler()', function () {
        it('should reject a non-function handler', function () {
            (() => query('select * from user').handler({}))
                .should.throw('handler must be a function');
        });

        it('should return the query', function () {
            const q = query('select * from accounts');
            q.should.equal(q.handler(_.noop));
        });

        it('should set the handler', function () {
            const q = query('select * from accounts').handler(_.noop);
            should.exist(q.defn.handler);
        });

        it('should bind the handler to an optional receiver', function () {
            const handler = function (query, reply) {
                should.exist(this);
                reply();
            };

            return query('select * from accounts')
                .handler(handler, {})
                .execute();
        });
    });

    describe('options()', function () {
        it('should set the query options', function () {
            const options = { foo: 'bar' };
            const handler = (q, r) => {
                q.options.should.deep.equal(options);
                r();
            };

            return query('select * from accounts')
                .handler(handler)
                .options(options)
                .execute();
        });
    });

    describe('option()', function () {
        it('should set the query options', function () {
            const handler = (q, r) => {
                q.options.should.deep.equal({ foo: 'bar', bar: 'baz' });
                r();
            };

            return query('select * from accounts')
                .handler(handler)
                .option('foo', 'bar')
                .option('bar', 'baz')
                .execute();
        });
    });

    describe('user()', function () {
        it('should set the user', function () {
            const user = { username: 'brad' };

            return query('select * from foo')
                .user(user)
                .handler((q, r) => {
                    q.user.should.deep.equal(user);
                    r();
                })
                .execute();
        });
    });

    describe('limit()', function () {
        it('should set the limit', function () {
            return query('select * from foo')
                .limit(1)
                .handler((q, r) => {
                    q.limit.should.equal(1);
                    r();
                })
                .execute();
        });

        it('should truncate the stream', function () {
            return query('select * from foo')
                .limit(1)
                .handler((q, r) => r(people))
                .toArray()
                .then(arr => arr.should.have.deep.members(people.slice(0, 1)));
        });
    });

    describe('field()', function () {
        it('should configure the query field defaults', function () {
            return query('select * from foo')
                .field('first', f => f.label('First Name'))
                .field('last', f => f.label('Last Name'))
                .handler((q, r) => r(people))
                .execute()
                .then(qr => {
                    _.get(qr, 'fields.first.label', '').should.equal('First Name');
                    _.get(qr, 'fields.last.label', '').should.equal('Last Name');
                });
        });

        it('should return a field configurator', function () {
            query('select * from foo')
                .field('first')
                .should.be.an.instanceOf(FieldConfigurator);
        });
    });

    describe('fields()', function () {
        it('should fully configure the fields', function () {
            return query('select * from foo')
                .fields({ first: { label: 'First Name' }, last: { label: 'Last Name' } })
                .handler((q, r) => r(people))
                .execute()
                .then(qr => {
                    _.get(qr, 'fields.first.label', '').should.equal('First Name');
                    _.get(qr, 'fields.last.label', '').should.equal('Last Name');
                });
        });

        it('should take an array short-hand', function () {
            return query('select * from foo')
                .fields(['first', 'last'])
                .handler((q, r) => r(people))
                .execute()
                .then(qr => qr.fields.should.deep.equal({
                    first: {
                        position: 0
                    },
                    last: {
                        position: 1
                    }
                }));
        });
    });

    describe('execute()', function () {
        it('should return a promise', function () {
            query('select * from accounts')
                .handler(_.noop)
                .execute()
                .should.respondTo('then');
        });

        it('should invoke the handler function', function () {
            const handler = sinon.spy(function (query, reply) {
                reply();
            });

            return query('select * from accounts')
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

            return query('select * from accounts where state = :state')
                .handler(handler)
                .execute({ state: 'NC' });
        });

        it('should return a shim from the reply function', function () {
            const handler = function (query, reply) {
                var shim = reply();
                shim.should.respondTo('field');
            };

            return query('select * from accounts where state = :state')
                .handler(handler)
                .execute();
        });

        it('should resolve to a query result object', function () {
            const handler = function (query, reply) {
                var shim = reply();
                shim.should.respondTo('field');
            };

            return query('select * from accounts where state = :state')
                .handler(handler)
                .execute()
                .then(qr => qr.should.be.an.instanceOf(QueryResult));
        });

        it('should emit an execute event', function () {
            const handler = function (query, reply) {
                reply([]);
            };

            const spy = sinon.spy();

            return query('select * from accounts')
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

            return query('select * from accounts')
                .handler(handler)
                .build()
                .on('result', spy)
                .execute()
                .then(r => spy.should.have.been.calledWith(r));
        });

        it('should emit a queryError event for a sync error in the handler', function () {
            let thrown;
            const error = new Error('Query handler sync error');
            const handler = () => {
                throw error;
            };

            const spy = sinon.spy();

            return query('select * fromwhere')
                .handler(handler)
                .build()
                .on('queryError', spy)
                .execute()
                .then(() => {
                    throw new Error('Should not succeed');
                })
                .catch(err => {
                    thrown = err;
                })
                .finally(() => {
                    should.exist(thrown);
                    thrown.should.equal(error);
                    spy.should.have.been.calledOnce;
                    spy.should.have.been.calledWith(error);
                });
        });

        it('should emit a queryError event for an error passed to the reply shim', function () {
            let thrown;
            const error = new Error('Query handler reply(error)');
            const spy = sinon.spy();

            return query('select * fromwhere')
                .handler((q, r) => r(error))
                .build()
                .on('queryError', spy)
                .execute()
                .then(() => {
                    throw new Error('Should not succeed');
                })
                .catch(err => {
                    thrown = err;
                })
                .finally(() => {
                    should.exist(thrown);
                    thrown.should.equal(error);
                    spy.should.have.been.calledOnce;
                    spy.should.have.been.calledWith(error);
                });
        });

        it('should include a post() function on the context object', function () {
            const handler = function (query, reply) {
                query.should.respondTo('post');
                reply();
            };

            return query('select * from accounts')
                .handler(handler)
                .execute();
        });

        it('should invoke the post function before returning the result', function () {
            const spy = sinon.spy();

            const handler = function (query, reply) {
                query.post(spy);
                reply();
            };

            return query('select * from accounts')
                .handler(handler)
                .execute()
                .then(r => spy.should.have.been.called);
        });
    });

    describe('stream()', function () {
        it('should return a stream', function (done) {
            const stream = query('select * from accounts')
                .handler((q, r) => r(people))
                .stream();

            should.exist(stream);
            stream.toArray(function (arr) {
                arr.should.have.deep.members(people);
                done();
            });
        });

        it('should stream a stream', function () {
            const stream1 = query()
                .handler((q, r) => r(people))
                .through([])
                .stream();

            return query()
                .handler((q, r) => r(stream1))
                .through([])
                .toArray()
                .then(arr => arr.should.have.deep.members(people));
        });
    });

    describe('pre()', function () {
        it('should invoke pre before calling handler', function () {
            const spy = sinon.spy();

            const handler = (q, r) => {
                spy.should.have.been.called;
                r(people);
            };

            return query('select * from accounts')
                .handler(handler)
                .pre(spy)
                .execute();
        });
    });

    describe('post()', function () {
        it('should invoke post before calling handler', function () {
            const spy = sinon.spy();

            const handler = (q, r) => {
                spy.should.not.have.been.called;
                r(people);
            };

            return query('select * from accounts')
                .handler(handler)
                .post(spy)
                .execute()
                .then(qr => spy.should.have.been.called);
        });
    });

    describe('through()', function () {
        it('should support stream transformation', function () {
            return query('select * from accounts')
                .handler((q, r) => r(people))
                .through(s => s.map(r => _.assign(r, { full: `${r.first} ${r.last}` })))
                .toArray()
                .then(arr => _.pluck(arr, 'full').should.have.members(['Brad Leupen', 'Hank Leupen']));
        });
    });

    describe('error()', function () {
        it('should invoke error handlers on a synchronous handler error', function () {
            let thrown;
            const error = new Error('Sync error');
            const handler0 = sinon.spy();
            const handler1 = sinon.spy();

            const run = () => query('select *fromasdfsdf')
                .handler((q, r) => {
                    throw error;
                })
                .error(handler0)
                .error(() => P.delay(500))
                .error(handler1)
                .execute();

            return run()
                .then(() => {
                    throw new Error('Should not have worked');
                })
                .catch(err => {
                    thrown = err;
                })
                .finally(() => {
                    should.exist(thrown);
                    thrown.should.equal(error);
                    handler0.should.have.been.calledOnce;
                    handler0.should.have.been.calledWith(thrown);
                    handler1.should.have.been.calledOnce;
                    handler1.should.have.been.calledWith(thrown);
                    return run()
                        .should.be.rejectedWith(error);
                });
        });

        it('should invoke error handlers on a handler reply error', function () {
            let thrown;
            const error = new Error('Sync error');
            const handler0 = sinon.spy();
            const handler1 = sinon.spy();

            const run = () => query('select *fromasdfsdf')
                .handler((q, r) => r(error))
                .error(handler0)
                .error(() => P.delay(500))
                .error(handler1)
                .execute();

            return run()
                .then(() => {
                    throw new Error('Should not have worked');
                })
                .catch(err => {
                    thrown = err;
                })
                .finally(() => {
                    should.exist(thrown);
                    thrown.should.equal(error);
                    handler0.should.have.been.calledOnce;
                    handler0.should.have.been.calledWith(thrown);
                    handler1.should.have.been.calledOnce;
                    handler1.should.have.been.calledWith(thrown);
                    return run()
                        .should.be.rejectedWith(error);
                });
        });
    });

    describe('cancel()', function () {
        it('should skip the handler if executed after cancellation', function () {
            const spy = sinon.spy();

            const q = query('select * from accounts')
                .handler(spy)
                .build();

            return q
                .cancel()
                .toArray()
                .then(function (arr) {
                    arr.should.have.lengthOf(0);
                    spy.should.not.have.been.called;
                });
        });
    });

    describe('use()', function () {
        it('should invoke the plugin', function () {
            const spy = sinon.spy();

            const q = query('select * from accounts')
                .handler((q, r) => r(people))
                .use(spy)
                .build();

            spy.should.have.been.calledWith(q);
        });
    });

    describe('language()', function () {
        it('should set the language', function () {
            query('select * from whatever')
                .handler((q, r) => r(people))
                .language('sql')
                .build()
                .should.have.property('language');
        });
    });

    describe('on()', function () {
        it('should forward event registrations to the query object', function () {
            const spy = sinon.spy();

            return query('select * from whatever')
                .handler((q, r) => r(people))
                .on('execute', spy)
                .execute()
                .then(r => spy.should.have.been.called);
        });
    });

    describe('configure()', function () {
        it('should configure the query', function () {
            const q = query()
                .handler((q, r) => r(people))
                .configure({ payload: 'select * from whatever', limit: 10, plugins: [sinon.spy()] }).build();
            q.should.have.property('payload', 'select * from whatever');
        });
    });
});

describe('Query', function () {
    describe('pre()', function () {
        it('should append to the pre handlers', function () {
            new Query('select * from whatever')
                .pre(_.noop)
                .should.have.property('preHandlers').that.has.lengthOf(1);
        });
    });

    describe('through()', function () {
        it('should append to the throughHandlers array', function () {
            new Query('select * from whatever')
                .through(_.noop)
                .should.have.property('throughHandlers').that.has.lengthOf(1);
        });
    });

    describe('error()', function () {
        it('should append to the errorHandlers array', function () {
            new Query('select foo frombar')
                .error(_.noop)
                .should.have.property('errorHandlers').that.has.lengthOf(1);
        });
    });

    describe('execute()', function () {
        it('should require a handler', function () {
            return new Query('select * from foo')
                .execute()
                .should.be.rejectedWith('No handler defined');
        });

        it('should only execute once', function () {
            const q = query('select * from whatever')
                .handler((q, r) => r(people))
                .build();

            q.execute().should.equal(q.execute());
        });

        it('should throw sync errors in the handler', function () {
            return query('select * from foo')
                .handler(() => {
                    throw new Error('Ooops!');
                })
                .execute()
                .should.be.rejectedWith('Ooops!');
        });

        it('should throw errors returned to reply', function () {
            return query('select * from foo')
                .handler((q, r) => r(new Error('Ooops!')))
                .execute()
                .should.be.rejectedWith('Ooops!');
        });
    });

    describe('use()', function () {
        it('should invoke the plugin', function () {
            const spy = sinon.spy();

            const q = query('select * from accounts')
                .handler((q, r) => r(people))
                .build()
                .use(spy);

            spy.should.have.been.calledWith(q);
        });
    });

    describe('field()', function () {
        it('should configure the query result field', function () {
            return query('select * from foo')
                .handler((q, r) => r(people))
                .build()
                .field('first', f => f.label('First Name'))
                .field('last', f => f.label('Last Name'))
                .execute()
                .then(qr => {
                    _.get(qr, 'fields.first.label', '').should.equal('First Name');
                    _.get(qr, 'fields.last.label', '').should.equal('Last Name');
                });
        });

        it('should return a field configurator', function () {
            return query('select * from foo')
                .handler((q, r) => r(people))
                .build()
                .field('first')
                .should.be.an.instanceof(FieldConfigurator);
        });
    });

    describe('progress()', function () {
        it('should emit a progress event', function () {
            const spy = sinon.spy();

            return query('select * from foo')
                .handler((q, r) => {
                    q.progress(1);
                    q.progress(2);
                    q.progress(3);
                    r(people);
                })
                .build()
                .on('progress', spy)
                .execute()
                .then(() => {
                    spy.should.have.been.calledThrice
                    spy.firstCall.args.should.have.members([1]);
                    spy.secondCall.args.should.have.members([2]);
                    spy.thirdCall.args.should.have.members([3]);
                });
        });
    });
});

describe('QueryResult', function () {
    describe('stream()', function () {
        it('should stream the data', function () {
            const handler = function (query, reply) {
                reply(people).fields(['firstName', 'lastName']);
            };

            return query('select * from person')
                .handler(handler)
                .execute()
                .then(qr => qr.stream())
                .then(s => new P(r => s.toArray(r)))
                .then(d => d.should.deep.equal(people));
        });

        it('should end a limited stream', function () {
            const spy = sinon.spy();

            return query()
                .handler((q, r) => r(people).on('end', spy))
                .limit(1)
                .toArray()
                .then(arr => {
                    arr.should.have.lengthOf(1);
                    spy.should.have.been.called;
                });
        });

        it(`should propagate in-stream errors occurring in the source`, async () => {
            let recNum = 0, written = [];
            const sourceError = new Error(`Source in-stream error. If this is thrown and not caught by pipeline, it blew up the domain`);
            const readableSource = new Readable({
                objectMode: true,
                read () {
                    if (this.__reading) return;
                    setTimeout(() => {
                        this.__reading = true;
                        if (recNum++ >= 9) return this.emit('error', sourceError);
                        this.push({ foo: Date.now() });
                        this.__reading = false;
                    });
                }
            });
            const queryResultEndSpy = sinon.spy();
            const result = await query()
                .handler((query, reply) => reply(null, readableSource)
                    .on('end', queryResultEndSpy))
                .execute();

            const writable = new Writable({
                objectMode: true,
                write (rec, enc, done) {
                    written.push(rec);
                    done();
                }
            });

            const $resultHighlandStream = result.stream();
            const pipelineSuccessSpy = sinon.spy();
            const pipelineErrorSpy = sinon.spy();
            await new P((resolve, reject) => {
                pipeline(
                    $resultHighlandStream,
                    writable,
                    err => {
                        if (err) return reject(err);
                        resolve(written);
                    }
                );
            })
                .then(pipelineSuccessSpy)
                .catch(pipelineErrorSpy);

            pipelineSuccessSpy.should.not.have.been.called;
            pipelineErrorSpy.should.have.been.calledOnce;
            pipelineErrorSpy.should.have.been.calledWithExactly(sourceError);
            recNum.should.equal(10);
            queryResultEndSpy.should.have.been.calledOnce;
        });

        it(`should properly reject call to toArray on an in-stream error`, async () => {
            let recNum = 0;
            const sourceError = new Error(`Source in-stream error. If this is not caught as queryResult.toArray() rejection, it blew up the domain`);
            const readableSource = new Readable({
                objectMode: true,
                read () {
                    if (this.__reading) return;
                    setTimeout(() => {
                        this.__reading = true;
                        if (recNum++ >= 9) return this.emit('error', sourceError);
                        this.push({ foo: Date.now() });
                        this.__reading = false;
                    });
                }
            });
            const queryResultEndSpy = sinon.spy();
            let caught;
            try {
                const result = await query()
                    .handler((query, reply) => reply(null, readableSource)
                        .on('end', queryResultEndSpy))
                    .execute();
                await result.toArray();
                should.not.exist(`Successful queryResult.toArray() call`);
            } catch (err) {
                caught = err;
            }
            should.exist(caught);
            caught.should.deep.equal(sourceError);
            recNum.should.equal(10);
        });
    });

    describe('toArray()', function () {
        it('should return a promise to a buffered array', function () {
            const handler = function (query, reply) {
                reply(people).fields(['firstName', 'lastName']);
            };

            return query('select * from person')
                .handler(handler)
                .execute()
                .then(qr => qr.toArray())
                .then(d => d.should.deep.equal(people));
        });
    });

    describe('cancel()', function () {
        it('should emit an end event in the handler via shim', function () {
            const spy = sinon.spy();
            return query()
                .handler(function (query, reply) {
                    let i = 0;
                    reply(function (push, next) {
                        push(null, i++);
                        next();
                    })
                        .on('end', spy);
                })
                .execute()
                .then(function (result) {
                    result.cancel();
                    return result.toArray();
                })
                .then(function (arr) {
                    arr.length.should.equal(0);
                    spy.should.have.been.called;
                });
        });

        it('should be called if the query is cancelled during execution', function () {
            const spy = sinon.spy();
            return query()
                .handler(function (query, reply) {
                    query.cancel();
                    let i = 0;
                    reply(function (push, next) {
                        push(null, i++);
                        next();
                    })
                        .on('end', spy);
                })
                .execute()
                .then(function (result) {
                    return result.toArray();
                })
                .then(function (arr) {
                    arr.length.should.equal(0);
                    spy.should.have.been.called;
                });
        });

        it('should be called if the query is cancelled after execution', function () {
            const spy = sinon.spy();
            var q = query()
                .handler(function (query, reply) {
                    query.cancel();
                    let i = 0;
                    reply(function (push, next) {
                        push(null, i++);
                        next();
                    })
                        .on('end', spy);
                })
                .build();

            return q.execute()
                .then(function (result) {
                    q.cancel();
                    return result.toArray();
                })
                .then(function (arr) {
                    arr.length.should.equal(0);
                    spy.should.have.been.called;
                });
        });

        it('should emit an end event on result stream', function (done) {
            query()
                .handler(function (query, reply) {
                    let i = 0;

                    reply(new Readable({
                        objectMode: true, read: function () {
                            setTimeout(() => this.push({ val: i++ }), 50);
                        }
                    }));
                })
                .execute()
                .tap(function (qr) {
                    let stream = qr.stream();
                    stream.on('end', done)
                        .pipe(new Writable({
                            objectMode: true, write: (r, e, d) => {
                                d();
                            }
                        }));
                })
                .then(qr => {
                    setTimeout(() => qr.cancel(), 1000);
                });
        });
    });

    describe('field()', function () {
        it('should configure the query result field', function () {
            return query('select * from foo')
                .handler((q, r) => r(people))
                .execute()
                .then(qr => {
                    qr.field('first', f => f.label('First Name'))
                        .field('last', f => f.label('Last Name'));

                    _.get(qr, 'fields.first.label', '').should.equal('First Name');
                    _.get(qr, 'fields.last.label', '').should.equal('Last Name');
                });
        });

        it('should return a field configurator', function () {
            return query('select * from foo')
                .handler((q, r) => r(people))
                .execute()
                .then(qr => {
                    qr.field('first').should.be.an.instanceof(FieldConfigurator);
                });
        });
    });

    describe('fieldDefaults()', function () {
        it('should fully configure the fields', function () {
            return query('select * from foo')
                .handler((q, r) => r(people))
                .execute()
                .then(qr => {
                    qr.fieldDefaults({ first: { label: 'First Name' }, last: { label: 'Last Name' } });
                    _.get(qr, 'fields.first.label', '').should.equal('First Name');
                    _.get(qr, 'fields.last.label', '').should.equal('Last Name');
                });
        });
    });

    describe('shim()', function () {
        var qr, shim;

        beforeEach(function () {
            var q = query('select * from accounts')
                .handler((q, r) => r(people))
                .build();

            qr = new QueryResult(q, people);
            shim = qr.shim();
        });

        it('should set the results fields', function () {
            shim.should.respondTo('fields');
            shim.fields(['first', 'last']);

            qr.fields.should.have.keys(['first', 'last']);
        });

        it('should set the selected number of rows', function () {
            shim.should.respondTo('selected');
            shim.selected(100);

            qr.selected.should.equal(100);
        });

        describe('field(<fieldName>)', function () {
            it('set the position', function () {
                var field = shim.field('foo');
                field.position(5);
                qr.fields.should.deep.equal({ foo: { position: 5 } });
            });

            it('set the label', function () {
                var field = shim.field('foo');
                field.label('Foo Bar');
                qr.fields.should.deep.equal({ foo: { label: 'Foo Bar' } });
            });

            it('should set the type', function () {
                var field = shim.field('foo');
                field.type('geo_point');
                qr.fields.should.deep.equal({ foo: { dataType: 'geo_point' } });
            });

            it('should support chaining', function () {
                shim.field('foo')
                    .label('FooBar')
                    .position(5)
                    .type('geo_point');

                qr.fields.should.deep.equal({
                    foo: {
                        label: 'FooBar',
                        position: 5,
                        dataType: 'geo_point'
                    }
                });
            });
        });

        describe('field(<fieldName>, <callback>)', function () {
            it('set the position', function () {
                shim.field('foo', f => f.position(5));
                qr.fields.should.deep.equal({ foo: { position: 5 } });
            });

            it('set the label', function () {
                shim.field('foo', f => f.label('Foo Bar'));
                qr.fields.should.deep.equal({ foo: { label: 'Foo Bar' } });
            });

            it('should set the type', function () {
                shim.field('foo', f => f.type('geo_point'));
                qr.fields.should.deep.equal({ foo: { dataType: 'geo_point' } });
            });

            it('should support chaining', function () {
                shim.field('foo', function (f) {
                    f
                        .label('FooBar')
                        .position(5)
                        .type('geo_point');
                });

                qr.fields.should.deep.equal({
                    foo: {
                        label: 'FooBar',
                        position: 5,
                        dataType: 'geo_point'
                    }
                });
            });

            it('should support chaining multiple fields', function () {
                shim.field('firstName', f => f.position(5))
                    .field('lastName', f => f.label('Last Name'))
                    .field('position', f => f.type('geo_point'));

                qr.fields.should.deep.equal({
                    firstName: { position: 5 },
                    lastName: { label: 'Last Name' },
                    position: { dataType: 'geo_point' }
                });
            });
        });
    });
});
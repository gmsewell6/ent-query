'use strict';

const _ = require('lodash');
const P = require('bluebird');
const EventEmitter = require('events').EventEmitter;
const flow = require('ent-flow');
const Logger = require('glib').Logger;
const log = new Logger(['query']);
const joi = require('joi');
const QueryResult = require('./query-result').QueryResult;

var $ = require('highland');

const SCHEMA = joi.object({
    preHandlers: joi.array().items(joi.func()).default([]),
    postHandlers: joi.array().items(joi.func()).default([]),
    throughHandlers: joi.array().items(joi.func()).default([]),
    handler: joi.func().required(),
    payload: joi.any(),
    user: joi.any(),
    fields: joi.object(),
    limit: joi.number().integer().default(-1),
    options: joi.object().default({}),
    params: joi.alternatives([ joi.object(), joi.array() ]).default({})
});

/**
 * @class
 * @property {function[]} throughHandlers
 */
class Query extends EventEmitter {
    constructor() {
        super();

        // properties declared here for jsdoc purposes
        this.handler = () => P.reject('No handler defined');
        this.limit = -1;
        this.preHandlers = this.postHandlers = this.throughHandlers = [];
        this.options = {};
        this.params = {};
    }

    /**
     * Configures the query from a defn descriptor
     * @param {{}} defn the definition descriptor
     * @return {Query}
     */
    configure(defn) {
         _.assign(this, joi.attempt(defn, SCHEMA));
        return this;
    }

    /**
     * Adds a through stream handler
     * @param through
     * @return {Query}
     */
    through(through) {
        this.throughHandlers = this.throughHandlers.concat(through);
        return this;
    }

    /**
     * Adds a pre handler
     * @param handler
     * @return {Query}
     */
    pre(handler) {
        this.preHandlers = this.preHandlers.concat(handler);
        return this;
    }

    /**
     * Adds a post handler
     * @param handler
     * @return {Query}
     */
    post(handler) {
        this.postHandlers = this.postHandlers.concat(handler);
        return this;
    }

    /**
     * Creates a promise chain for invoking interceptors, ignoring any return value
     * @param {[]} interceptors the interceptor functions
     * @return {Function}
     * @private
     */
    _tapChain(interceptors) {
        return function(value) {
            return interceptors.reduce(function (p, interceptor) {
                return p.tap(interceptor);
            }, P.resolve(value));
        };
    }

    /**
     * A new limiting stream if limit is defined
     * @return {*}
     * @private
     */
    _limitStream() {
        return this.limit >= 0 ? s => s.take(this.limit) : _.identity;
    }

    /**
     * Invokes the handler function
     * @return {Promise.<QueryResult>}
     * @private
     */
    _invokeHandler() {
        const self = this;
        const handler = this.handler;

        return new P(function (resolve) {
            const reply = _.once(function (data) {
                const qr = new QueryResult(self)
                    .data(data)
                    .fieldDefaults(self.fields)
                    .through(self._limitStream())
                    .through(self.throughHandlers);

                setImmediate(() => resolve(qr));
                return qr.shim();
            });

            //noinspection JSCheckFunctionSignatures
            handler(self, reply);
        });
    }

    /**
     * Runs the query with the supplied parameters, returning a promise to a QueryResult
     * @return {Promise.<QueryResult>}
     */
    execute() {
        if (this._promise) return this._promise;

        return this._promise = P.bind(this)
            .tap(() => log.info('Executing query'))
            .tap(() => this.queryStartedAt = new Date())
            .tap(() => this.emit('execute'))
            .tap(() => this._tapChain(this.preHandlers)(this))
            .then(() => this._invokeHandler())
            .tap(() => this.queryEndedAt = new Date())
            .tap(r => this._tapChain(this.postHandlers)(r))
            .tap(r => this.emit('result', r));
    }

    /**
     * Executes the query, synchronously returning a stream bound to the result's stream
     * @return {Stream}
     */
    stream() {
        return $.sequence(this.execute().then(r => r.stream()));
    }

    /**
     * Executes the query returning a promise that resolves to an array containing all results
     * @return {Promise.<{}[]>}
     */
    toArray() {
        return this.execute().then(r => r.toArray());
    }
}


exports.Query = Query;
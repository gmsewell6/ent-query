'use strict';

const _ = require('lodash');
const P = require('bluebird');
const EventEmitter = require('events').EventEmitter;
const Logger = require('glib').Logger;
const log = new Logger(['query']);
const joi = require('joi');
const QueryResult = require('./query-result').QueryResult;
const FieldConfigurator = require('./field-configurator').FieldConfigurator;

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
    language: joi.string(),
    options: joi.object().default({}),
    params: joi.alternatives([ joi.object(), joi.array() ]).default({}),
    plugins: joi.array().items(joi.func()).default([])
});

/**
 * @extends EventEmitter
 */
class Query extends EventEmitter {
    constructor() {
        super();

        // properties declared here for jsdoc purposes
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
        this.plugins.forEach(p => p(this));
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
     * Registers a plugin with the query
     * @param {function} plugin the plugin registration function
     * @return {Query}
     */
    use(plugin) {
        this.plugins = this.plugins.concat(plugin);

        plugin(this);

        return this;
    }

    /**
     * Configures a field
     * @param {string} field the field name
     * @param {function=} callback an optional callback function for accessing the field configurator
     * @example
     * // with a callback
     * qr
     *  .field('location', fc => fc.type('geo_point').label('Coordinates'))
     *  .field('firstName', fc => fc.label('First Name'));
     *  .field('lastName', fc => fc.label('Last Name'));
     *
     *  // without a callback
     *  qr.field('location').type('geo_point');
     *  qr.field('firstName').label('First Name');
     *  qr.field('lastName').label('Last Name');
     *
     * @return {FieldConfigurator|Query}
     */
    field(field, callback) {
        const fc = new FieldConfigurator(this.fields, field);
        if (callback) {
            callback(fc);
            return this;
        }

        return fc;
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
     * Invokes the query's handler function and is responsible for initializing the result
     * @return {Promise.<QueryResult>}
     * @private
     */
    _invokeHandler() {
        const self = this;
        const handler = this.handler;

        // short circuit the handler if already cancelled
        if (this.cancelled) return new QueryResult(this);

        return new P(function (resolve, reject) {
            const reply = _.once(function (err, data) {
                if (err && err instanceof Error) return reject(err);

                const qr = new QueryResult(self, data || err);

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

        if (!this.handler) return P.reject('No handler defined');

        return this._promise = P.bind(this)
            .tap(() => log.info('Executing query'))
            .tap(() => this.emit('execute'))
            .tap(() => this._tapChain(this.preHandlers)(this))
            .then(() => this._invokeHandler())
            .tap(r => this.result = r)
            .tap(r => this._tapChain(this.postHandlers)(r))
            .tap(r => this.emit('result', r));
    }

    /**
     * Cancels the query. It's up to listeners to decide what this means.
     */
    cancel() {
        this.cancelled = true;

        // once the result returns, its too late to abort
        if (this.result) {
            this.result.cancel();
        } else {
            this.emit('cancel');
        }

        return this;
    }

    /**
     * Emits a progress event
     * @param progress
     * @return {boolean|*}
     */
    progress(progress) {
        return this.emit('progress', progress);
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
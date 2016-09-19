'use strict';

const _ = require('lodash');
const P = require('bluebird');
const EventEmitter = require('events').EventEmitter;
const Logger = require('glib').Logger;
const log = new Logger(['query']);
const joi = require('joi');
const Stream = require('stream').Stream;
const QueryResult = require('./query-result').QueryResult;
const FieldConfigurator = require('./field-configurator').FieldConfigurator;

var $ = require('highland');

const SCHEMA = joi.object({
    preHandlers: joi.array().items(joi.func()).default([]),
    postHandlers: joi.array().items(joi.func()).default([]),
    throughHandlers: joi.array().items(joi.alternatives([ joi.func(), joi.object().type(Stream) ])).default([]),
    handler: joi.func().required(),
    payload: joi.any(),
    user: joi.any(),
    fields: joi.object(),
    limit: joi.number().integer().default(-1),
    language: joi.string(),
    options: joi.object().default({}),
    params: joi.alternatives([ joi.object(), joi.array() ]).default({}),
    plugins: joi.array().items(joi.func()).default([]),
    listeners: joi.array().items(joi.array().items(joi.string(), joi.func())).default([])
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
        defn = joi.attempt(defn, SCHEMA);
         _.assign(this, _.omit(defn, 'plugins', 'listeners'));
        defn.plugins.forEach(p => p(this));
        defn.listeners.forEach(l => this.on(l[0], l[1]));
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
     * @param {function(Query, function)} handler the handler function
     * @return {Promise.<QueryResult>}
     * @private
     */
    _invokeHandler(handler) {
        const self = this;

        // short circuit the handler if already cancelled
        if (this.cancelled) return new QueryResult(this);

        return new P(function (resolve, reject) {

            // the reply function passed to the query handler
            const reply = _.once(function (err, data) {

                // errors thrown in the handler will have same effect
                if (err && err instanceof Error) return reject(err);

                // init the result
                const qr = new QueryResult(self, data || err);

                // give the handler a moment to configure field metadata before processing the results
                setImmediate(() => resolve(qr));

                // return the configuration shim
                return qr.shim();
            });

            self.on('cancel', reply);
            handler(self, reply);
        });
    }

    _setResult(result) {
        this.result = result;
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
            .then(() => this._invokeHandler(this.handler))
            .tap(r => this._setResult(r))
            .tap(r => this._tapChain(this.postHandlers)(r))
            .tap(r => this.emit('result', r));
    }

    /**
     * Cancels a running query. NOTE: this method is overridden to call QueryResult.cancel() once the reply() callback
     * is called.
     *
     * Handlers should detect early termination of queries in two ways:
     *
     * <pre>
     *     function handler(query, reply) {
     *          let conn = getConnection();
     *
     *          // a cancellation event on query indicates that the query is still running (we haven't called reply yet)
     *          query.on('cancel', () => conn.end())
     *
     *          conn.query(query.payload)
     *              .then(function(result) {
     *                  // an end event on the reply is emitted when the results are consumed or the client cancels
     *                  // the query result
     *                  reply(result.stream()).on('end', () => conn.end());
     *              });
     *     }
     * </pre>
     */
    cancel() {
        this.cancelled = true;
        this.emit('cancel');
        if (this.result) this.result.cancel();
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
     * Executes the query, synchronously returning a stream bound (eventually) to the result's stream
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
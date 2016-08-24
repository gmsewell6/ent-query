'use strict';

const _ = require('lodash');
const P = require('bluebird');
const EventEmitter = require('events').EventEmitter;
const flow = require('ent-flow');
const Logger = require('glib').Logger;
const log = new Logger(['query']);

var $ = require('highland');

/**
 * A query handler callback
 * @callback handlerCallback
 * @param {{}} query the query to execute
 * @param {function} reply the reply
 */

/**
 * A utility for configuring field entries in a field hash
 */
class FieldConfigurator {

    /**
     * Constructor
     * @param {{}} fields the fields object keyed by field name
     * @param {string} field the name of the field to configure
     */
    constructor(fields, field) {
        this.fields = fields;
        this.field = field;
    }

    /**
     * Sets the elasticsearch type mapping
     * @param {string} typeMapping the full elasticsearch type mapping
     * @return {FieldConfigurator} the field configurator instance for chaining
     */
    typeMapping(typeMapping) {
        return this.configure({ typeMapping: typeMapping });
    }

    /**
     * Shorthand for simple type mappings
     * @param {string} type the elasticsearch type
     * @example
     * query.field('location').type('geo_point');
     * @return {FieldConfigurator} the field configurator instance for chaining
     */
    type(type) {
        return this.configure({ typeMapping: { type: type } });
    }

    /**
     * Assigns a label to the selected field
     * @param {string} label the field label
     * @example
     * query.field('location').label('Coordinates')
     * @return {FieldConfigurator} the field configurator instance for chaining
     */
    label(label) {
        return this.configure({ label: label });
    }

    /**
     * Sets the default field position
     * @param {number} position the field's position
     * @example
     * query.field('location').position(0)
     * @return {FieldConfigurator} the field configurator instance for chaining
     */
    position(position) {
        return this.configure({ position: position });
    }

    /**
     * Fully configures a field all at once
     * @param {{}} config the field config
     * @example
     * query.field('location').configure({ label:
     * @return {FieldConfigurator} the field configurator instance for chaining
     */
    configure(config) {
        self.fields[this.field] = _.assign({}, self.fields[this.field], config);
        return this;
    }
}

/**
 * The result of a query. Houses any metadata returned by the driver and provides an interface to begin streaming
 * the result records.
 */
class QueryResult {
    /**
     * Constructs a new QueryResult instance. Usually only called by the invokeWithReply static function
     * @param {{}} query the query object
     */
    constructor(query) {
        this.query = query;
        this.fields = {};
        this._through = [];
        this.data([]);
    }

    data(data) {
        this._source = $(data);
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
     * @return {FieldConfigurator|QueryResult}
     */
    field(field, callback) {
        const fc = new FieldConfigurator(this.fields, field);

        // hit the call back for chaining
        if (callback) {
            callback(fc);
            return this;
        }

        return fc;
    }

    /**
     * Assigns default configuration to fields
     * @param defaults
     * @return {QueryResult}
     */
    fieldDefaults(defaults) {
        _.defaults(this.fields, defaults);
        return this;
    }

    /**
     * Adds a new stream factory to the end of the through stream
     * @param {function} through
     * @example
     * qr.through(s => s.filter(filterFn));
     * @return {QueryResult}
     */
    through(through) {
        this._through = this._through.concat(through);
        return this;
    }

    /**
     * Shims the query result for synchronous configuration by the query handler
     * @return {{fields: fields, selected: selected, field: field}}
     */
    shim() {
        var self = this;

        return {
            // configures multiple fields
            fields: function (value) {
                value = _.isArray(value) ? _.object(value.map(v => [v, {}])) : value;
                self.fields = _.assign({}, value);
                return this;
            },

            // sets the number of selected records (not all records may actually be streamed)
            selected: function (value) {
                self.selected = value;
                return this;
            },

            // configures a specific field
            field: function (field, callback) {
                var config = self.field(field);
                if (callback) {
                    callback(config);
                    return this;
                }
                return config;
            }
        };
    }

    /**
     * Creates a data flow by mapping over the source stream through the through stream mappers
     * @return {Object}
     */
    stream() {
        const streams = []
            .concat(_.constant(this._source))
            .concat(this._through);

        // the query result instance will be the context variable passed to all through streams
        return flow.create(this, streams).stream();
    }

    /**
     * Returns a promise that resolves to an array of all data values
     */
    toArray() {
        return new P(resolve => this.stream().toArray(resolve));
    }

    /**
     * Invokes a handler function with the query and a constructed reply interface
     * @param {{}} query the query being executed
     * @param {handlerCallback} handler a handler function that accepts query and reply arguments
     *
     * @return {Promise.<QueryResult>} a promise that is resolved once the query handler invokes the reply callback
     */
    static invokeWithReply(query, handler) {
        return new P(function (resolve) {
            const reply = _.once(function (data) {
                const qr = new QueryResult(query).data(data);

                setImmediate(() => resolve(qr));
                return qr.shim();
            });

            handler(query, reply);
        });
    }
}

/**
 * A DSL for building queries and executing them via a "handler" function. This class makes no assumptions about the
 * query definition, or payload, or the format of the result, as long as it can be coerced into an object stream.
 *
 * Query result streams may be intercepted and modified through the use of through stream mapper functions. Each
 * function is passed the current stream and a QueryResult instance.
 *
 * Queries may be executed multiple times, generating a new response and result stream each time.
 *
 * @example
 * <pre>
 * function handler(query, reply) {
 *     // reply() result may be used to provide additional metadata
 *     reply([ { first: 'John', last: 'Doe' }, { first: 'Jane', last: 'Doe' } ])
 *          .field('first', f => f.label('First Name'))
 *          .field('last', f => f.label('Last Name'));
 * }
 *
 * new Query('select * from my_table')
 *      .handler(handler)
 *      .through(s => s.map(r => _.assign(r, { fullName: `${r.first} ${r.last}` })))
 *      .execute()
 *      .then(results => results.stream()) // results have populated field objects
 * </pre>
 */
class Query extends EventEmitter {
    constructor(payload) {
        super();
        this.ctx = { payload: payload, options: {} };
        this._through = [];
        this._fields = {};
        this._limitStream = _.identity;
    }

    /**
     * Creates a new query context descriptor for each query execution. The context object is used to pass information
     * into the handler.
     * @param {{}, []=} params optional parameters hash or array to be sent to the handler
     * @private
     */
    _newContext(params) {
        return _.defaults({ params: params, time: new Date() }, this.ctx);
    }

    /**
     * Invokes the handler function
     * @param {{}, []=} params optional parameters hash or array to be sent to the handler
     * @return {Promise.<QueryResult>}
     * @private
     */
    _invokeHandler(params) {
        if (!this._handler) throw new Error('No handler defined. Did you forget to call handler()?');

        const ctx = this._newContext(params);

        this.emit('execute', ctx);

        return QueryResult.invokeWithReply(this._newContext(params), this._handler);
    }

    /**
     * Performs a live (non-cached) query against the handler. The results are optionally written to cache
     * @param params
     * @private
     */
    _query(params) {
        return P.resolve()
            .then(() => this._invokeHandler(params))
            .tap(r => this._writeToCache(r));
    }

    /**
     * Writes query result to cache if appropriate
     * @param {QueryResult} result
     * @return {*}
     * @private
     */
    _writeToCache(result) {
        return this._cache && this._cacheKey && this._cache.write(this._cacheKey, result);
    }

    /**
     * Reads a query result from cache if appropriate
     * @param {{}, []=} params query params
     * @return {Promise.<QueryResult>}
     * @private
     */
    _readFromCache(params) {
        if (!this._cache || !this._cacheKey) return;

        return this._cache.exists(this._cacheKey)
            .then(exists => exists && QueryResult.invokeWithReply(this._newContext(params), (q, r) => this._cache.read(this._cacheKey, r)));
    }

    /**
     * Configures the final query result, live or cached, before returning it to the client
     * @param {QueryResult} queryResult
     * @return {QueryResult}
     * @private
     */
    _configureResult(queryResult) {
        return queryResult.fieldDefaults(this._fields)
            .through(this._limitStream)
            .through(this._through);
    }

    /**
     * Sets query's handler function, which is called when the query is executed
     * @param {handlerCallback} handler the handler function
     * @param {{}=} receiver an optional receiver to bind to the handler
     */
    handler(handler, receiver) {
        if (!_.isFunction(handler)) throw new Error('handler must be a function');

        // bind the handler to its receiver, if the handler function is a method
        this._handler = receiver ? handler.bind(receiver) : handler;

        return this;
    }

    /**
     * Sets handler options
     * @param {{}} options
     * @return {Query}
     */
    options(options) {
        this.ctx.options = _.assign({}, options);
        return this;
    }

    /**
     * Sets a single handler option
     * @param {string} name the option name
     * @param {*} value the option value
     * @example
     * @return {Query}
     */
    option(name, value) {
        this.ctx.options = _.set(this.ctx.options, name, value);
        return this;
    }

    /**
     * Appends one or more stream mappers to the through chain
     * @param {function|function[]} streams the stream mappers
     * @return {Query}
     */
    through(streams) {
        this.ctx.through = this.ctx.through.concat(streams);
        return this;
    }

    /**
     * Sets the cache for the query. Cache instances must implement methods for reading and writing query results
     * @param cache
     * @return {Query}
     */
    cache(cache) {
        if (!_.isFunction(cache.exists)) throw new Error('Cache must implement exists()');
        if (!_.isFunction(cache.read)) throw new Error('Cache must implement read()');
        if (!_.isFunction(cache.write)) throw new Error('Cache must implement write()');

        this._cache = cache;

        return this;
    }

    /**
     * Sets the cache key
     * @param cacheKey
     * @return {Query}
     */
    cacheKey(cacheKey) {
        this._cacheKey = cacheKey;
        return this;
    }

    /**
     * Sets the user
     * @param {*} user
     * @return {Query}
     */
    user(user) {
        this.ctx.user = user;
        return this;
    }

    /**
     * Sets a limit option. The limit is conveyed to the handler for potential query optimizations (e.g. '... LIMIT ').
     * In addition, a limit stream is constructed to halt stream iteration when limit is reached
     *
     * @param limit
     * @return {Query}
     */
    limit(limit) {
        this.ctx.limit = limit;
        this._limitStream = s => s.take(limit);
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
     * @return {FieldConfigurator|QueryResult}
     */
    field(field, callback) {
        const fc = new FieldConfigurator(this._fields, field);
        if (callback) {
            callback(fc);
            return this;
        }

        return fc;
    }

    /**
     * Sets the full field configuration
     * @param fields
     * @return {Query}
     */
    fields(fields) {
        fields = _.isArray(fields) ? _.object(fields.map(v => [v, {}])) : value;
        self.fields = _.assign({}, fields);
        return this;
    }

    /**
     * Runs the query with the supplied parameters, returning a promise to a QueryResult
     * @param {{}, []=} params optional query params
     * @return {Promise.<QueryResult>}
     */
    execute(params) {
        return P.resolve()
            .then(() => this._readFromCache(params))
            .catch(err => log.warn('cache', err))
            .then(r => r || this._query(params))
            .then(r => this._configureResult(r))
            .tap(r => this.emit('result', r));
    }

    /**
     * Executes the query, synchronously returning a stream bound to the result's stream
     * @param {{}, []=} params optional query params
     * @return {Stream}
     */
    stream(params) {
        return $.sequence(this.execute(params).then(r => r.stream()));
    }

    /**
     * Executes the query returning a promise that resolves to an array containing all results
     * @param {{}, []=} params optional query params
     * @return {Promise.<{}[]>}
     */
    toArray(params) {
        return this.execute(params).then(r => r.toArray());
    }
}

exports.Query = Query;
exports.QueryResult = QueryResult;
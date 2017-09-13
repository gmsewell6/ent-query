'use strict';

const _ = require('lodash');
const Query = require('./query').Query;
const FieldConfigurator = require('./field-configurator').FieldConfigurator;

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
class QueryBuilder {
    constructor(payload) {
        this.defn = {
            fields: {},
            payload: payload,
            options: {},
            dataTypes: {},
            preHandlers: [],
            postHandlers: [],
            throughHandlers: [],
            errorHandlers: [],
            plugins: [],
            listeners: []
        };
    }

    /**
     * Sets query's handler function, which is called when the query is executed
     * @param {function(Query, function)} handler the handler function
     * @param {{}=} receiver an optional receiver to bind to the handler
     */
    handler(handler, receiver) {
        if (!_.isFunction(handler)) throw new Error('handler must be a function');

        // bind the handler to its receiver, if the handler function is a method
        this.defn.handler = receiver ? handler.bind(receiver) : handler;

        return this;
    }


    /**
     * Sets handler options
     * @param {{}} options
     * @return {QueryBuilder}
     */
    options(options) {
        this.defn.options = _.assign({}, options);
        return this;
    }

    /**
     * Sets a single handler option
     * @param {string} name the option name
     * @param {*} value the option value
     * @example
     * @return {QueryBuilder}
     */
    option(name, value) {
        this.defn.options = _.set(this.defn.options, name, value);
        return this;
    }

    /**
     * Sets the user
     * @param {*} user
     * @return {QueryBuilder}
     */
    user(user) {
        this.defn.user = user;
        return this;
    }

    /**
     * Sets a limit option. The limit is conveyed to the handler for potential query optimizations (e.g. '... LIMIT ').
     * In addition, a limit stream is constructed to halt stream iteration when limit is reached
     *
     * @param limit
     * @return {QueryBuilder}
     */
    limit(limit) {
        this.defn.limit = limit;
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
     * @return {FieldConfigurator|QueryBuilder}
     */
    field(field, callback) {
        const fc = new FieldConfigurator(this.defn.fields, field);
        if (callback) {
            callback(fc);
            return this;
        }

        return fc;
    }

    /**
     * Sets the full field configuration
     * @param fields
     * @return {QueryBuilder}
     */
    fields(fields) {
        fields = _.isArray(fields) ? _.object(fields.map((v, i) => [v, { position: i }])) : fields;
        this.defn.fields = _.assign({}, fields);
        return this;
    }

    /**
     * Sets a hash of valid types
     * @param types
     */
    types(types) {
        this.defn.dataTypes = types;
        return this;
    }

    /**
     * Creates a new query instance by merging the current defaults with the specified params
     * @param params
     * @return {Query}
     */
    build(params) {
        const defn = _.assign(this.defn, { params: params });

        return new Query().configure(defn);
    }

    /**
     * Runs the query with the supplied parameters, returning a promise to a QueryResult
     * @param {{}, []=} params optional query params
     * @return {Promise.<QueryResult>}
     */
    execute(params) {
        return this.build(params).execute();
    }

    /**
     * Executes the query, synchronously returning a stream bound to the result's stream
     * @param {{}, []=} params optional query params
     * @return {Stream}
     */
    stream(params) {
        return this.build(params).stream();
    }

    /**
     * Executes the query returning a promise that resolves to an array containing all results
     * @param {{}, []=} params optional query params
     * @return {Promise.<{}[]>}
     */
    toArray(params) {
        return this.build(params).toArray();
    }

    /**
     * Applies a pre interceptor
     * @param interceptor
     * @return {QueryBuilder}
     */
    pre(interceptor) {
        this.defn.preHandlers = this.defn.preHandlers.concat(interceptor);
        return this;
    }

    /**
     * Applies a post interceptor
     * @param interceptor
     * @return {QueryBuilder}
     */
    post(interceptor) {
        this.defn.postHandlers = this.defn.postHandlers.concat(interceptor);
        return this;
    }

    /**
     * Appends one or more stream mappers to the through chain
     * @param {function|function[]} streams the stream mappers
     * @return {QueryBuilder}
     */
    through(streams) {
        this.defn.throughHandlers = this.defn.throughHandlers.concat(streams);
        return this;
    }

    /**
     * Applies an error interceptor
     * @param interceptor
     * @return {QueryBuilder}
     */
    error(interceptor) {
        this.defn.errorHandlers = this.defn.errorHandlers.concat(interceptor);
        return this;
    }

    /**
     * Sets the query language of the payload
     * @param {string} language
     * @return {QueryBuilder}
     */
    language(language) {
        this.defn.language = language;
        return this;
    }

    /**
     * Registers a plugin
     * @param {function|function[]} plugin the plugin registration function or functions
     * @return {QueryBuilder}
     */
    use(plugin) {
        this.defn.plugins.push(plugin);
        return this;
    }

    /**
     * Registers an event listener for queries created by this builder
     * @param {string} event the event name
     * @param {function} handler the event handler
     * @return {QueryBuilder}
     */
    on(event, handler) {
        this.defn.listeners.push([event, handler]);
        return this;
    }

    /**
     * Configures the current definition
     * @param config
     * @return {QueryBuilder}
     */
    configure(config) {
        _.merge(this.defn, config);
        return this;
    }
}

exports.QueryBuilder = QueryBuilder;
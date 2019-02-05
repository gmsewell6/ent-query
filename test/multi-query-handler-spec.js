'use strict';

const query = require('../lib');
const { Readable, Writable } = require('stream');
const multi = require('../lib/multi-query-handler');
const sinon = require('sinon');

describe('multiHandler', function () {

    it('should stream all the data', async function () {
        const handler = function (query, reply) {
            reply(query.payload)
                .fields([ 'customer', 'date', 'amount' ])
                .selected(query.payload.length);
        };

        const data = await query([
            {
                id: '1997',
                payload: [ {
                    customer: 'Brad',
                    date: '1-1-1997',
                    amount: 100.00
                } ]
            },
            {
                id: '1998',
                payload: [ { customer: 'Hank', date: '1-1-1998', amount: 200.00 } ]
            }
        ])
            .handler(multi(handler, { discriminator: 'year' }))
            .toArray();

        data.should.have.deep.members([
            { customer: 'Brad', date: '1-1-1997', amount: 100.00, year: '1997' },
            { customer: 'Hank', date: '1-1-1998', amount: 200.00, year: '1998' }
        ])
    });

    it('should forward query cancellations', async function () {
        let cancelled = false;
        const handler = function (query, reply) {
            query.on('cancel', () => cancelled = true );
            let cnt = 0;
            const stream = new Readable({
                objectMode: true,
                read: function () {
                    if (!cancelled) this.push({ id: cnt++ });
                }
            });
            reply(stream);
        };
        const q = query({}).handler(multi(handler)).build();
        const qr = await q.execute({});
        try {
            const stream = await qr.stream();
            await new Promise(resolve => {
                stream.pipe(new Writable({
                    objectMode: true,
                    write: () => {
                        q.cancel();
                        resolve();
                    }
                }))
            });
        } catch (err) {

        }
        cancelled.should.be.true;
    });

    it.only('should propagate end events', async function () {
        let spy = sinon.spy();
        const handler = function (query, reply) {
            let cnt = 0;
            const stream = new Readable({
                objectMode: true,
                read: function () {
                    this.push({ id: cnt++ });
                }
            });
            reply(stream).on('end', spy);
        };
        const q = query({}).handler(multi(handler)).limit(5).build();
        const values = await q.toArray();
        values.should.have.length(5);
    });
});
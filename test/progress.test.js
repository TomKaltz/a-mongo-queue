const test = require('ava')
// const MockDate = require('mockdate')

const setup = require('./setup.js')
const mongoDbQueue = require('../')
const queueName = 'progress'

let db
let q

test.before('setup db', async t => {
  db = await setup(queueName)
  q = await mongoDbQueue(db, queueName)
})

test('returns a promise', t => {
  let res = q.progress('doesnotexist')
  res.catch(()=>{})
  t.is(res.constructor.name, 'Promise', 'returns a promise object')
  t.is(typeof res.then, 'function', 'returns a thanable')
})

test('rejects promise if the id does not exist', async t => {
  await t.throws(q.progress('thisiddoesnotexist'))
})

test('rejects promise if the msg was already acked', async t => {
  await q.enqueue()
  let msg = await q.dequeue()
  await q.ack(msg.id)
  await t.throws(q.progress(msg.id))
})

test('rejects promise if the msg was already failed', async t => {
  await q.enqueue()
  let msg = await q.dequeue()
  await q.fail(msg.id)
  await t.throws(q.progress(msg.id))
})

test('test params and that progress is stored in database', async t => {
  await q.enqueue()
  let msg = await q.dequeue()
  t.deepEqual(msg.progress, 0)
  await t.throws(q.progress(msg.id, 'thiswillfail'))
  await t.notThrows(q.progress(msg.id, -5))
  await t.notThrows(q.progress(msg.id, 0))
  await t.notThrows(q.progress(msg.id, 50))
  let getRes = await q.get(msg.id)
  t.deepEqual(getRes.progress, 50)
  await t.notThrows(q.progress(msg.id, 95.5))
  getRes = await q.get(msg.id)
  t.deepEqual(getRes.progress, 95.5)
  await t.notThrows(q.progress(msg.id, 100))
  await t.notThrows(q.progress(msg.id, 105))
  getRes = await q.get(msg.id)
  t.deepEqual(getRes.progress, 100)
  await q.ack(msg.id)
})

test.after('close the db conn', t => {
  db.close()
})
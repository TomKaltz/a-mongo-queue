process.env.DEBUG = '*'

const setup = require('../test/setup')
const mongoDbQueue = require('../')

async function run(){
  let db = await setup('process-concurrency')
  let q = await mongoDbQueue(db, 'process-concurrency-dev')
  let ids = await q.enqueue([{}, {}, {}, {}, {}])
  q.process(async (q, msg)=>{
    console.log('waiting 1 sec', msg.id)
    // await new Promise(resolve => setTimeout(resolve, 1000))
    // console.log('returning from func after waiting 1 sec', msg.id)
    return 'something: '+ msg.id
  },{concurrency: 1})

  // setTimeout(async ()=>{
  //   console.log('attempting to stop')
  //   await q.stop(true)
  //   console.log('q has stopped')
  //   await db.close()
  //   console.log('db has closed')
  // },2200)
  
}

run()


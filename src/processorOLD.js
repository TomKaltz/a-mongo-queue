module.exports = Processor

function Processor(q, fn, opts = {}) {
  if(!q || q.constructor.name !== 'Queue') throw new Error('Processor: you must pass in a valid Queue object')
  if(!fn) throw new Error('Processor: you must pass in a function')
  let running = false
  let concurrency = opts.concurrency || 1
  let pollInterval = opts.pollInterval || 2000
  let lockDuration = opts.lockDuration || q.lockDuration
  let inFlight = []
  let noOp = ()=>{}
  let cancelWait = noOp

  const start = () => {
    if(!running){
      running = true
      run()
    }
  }
  
  const stop = async graceful => {
    console.log('stop - graceful:',graceful)
    running = false
    cancelWait()
    if(graceful){
      let all = Promise.all(inFlight)
      all.catch(()=>{console.log('something went wrong while awaiting the inflight messages')})
      await all
      console.log('all inflight functions have returned', inFlight.length)
      return true
    }
    return false
  }

  const createFinisher = (isError, msg) => {
    let finFunc = function (result){
      let p
      console.log('finishing', msg.id, 'inFlight', inFlight.length)
      if(isError){
        p = q.fail(msg.id, result)
      } else {
        p = q.ack(msg.id)
      }
      let index = inFlight.indexOf(finFunc)
      if(index>-1){
        inFlight.splice(index, 1)
      }
      cancelWait()
      return p
    }
    return finFunc
  }

  const getCancellableTimeout =  (delay) => {
    return new Promise ((resolve) =>{
      let to = setTimeout(()=>{
        cancelWait = noOp
        resolve()
      },delay)
      cancelWait = ()=>{
        console.log('canceling wait', 'inFlight', inFlight.length)
        clearTimeout(to)
        cancelWait = noOp
        resolve()
      }
    })
  }

  const run = async () => {
    while(running){
      console.log('filling slots', 'inFlight', inFlight.length)
      while (inFlight.length < concurrency){
        console.log('polling queue', 'inFlight', inFlight.length)
        let msg = await q.dequeue({lockDuration})
        if(msg) {
          console.log('calling func for msg', msg.id)
          let p = fn(q, msg)
            .catch(createFinisher(true, msg))
            .then(createFinisher(false, msg))
          inFlight.push(p)
        } else {
          console.log('queue empty...waiting', pollInterval, 'inFlight', inFlight.length)
          if(running)
            await getCancellableTimeout(pollInterval)
        }
      }
      console.log('all slots full...waiting', pollInterval, 'inFlight', inFlight.length)
      if(running)
        await getCancellableTimeout(pollInterval)
    }
    console.log('running was set to false', inFlight.length, concurrency)
  }
  
  start()
  console.log('returning from processor.js')
  return {
    start,
    stop,
  }
}
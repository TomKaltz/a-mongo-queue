module.exports = {
  oncePromise(input, expectedEventName) {
    return new Promise(resolve => {
      input.once(expectedEventName, resolve)
    })
  },
}
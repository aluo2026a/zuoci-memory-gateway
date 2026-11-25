const fastify = require('fastify')({ logger: true })

console.log("==== ENV DEBUG ====")
console.log("process.env.PORT =", process.env.PORT)
console.log("process.env.RAILWAY_PORT =", process.env.RAILWAY_PORT)

const PORT = parseInt(process.env.PORT || 3000, 10)

fastify.get('/', async () => {
  return { msg: "Memory Gateway Alive ✅", portGot: PORT }
})

const start = async () => {
  try {
    // 改这里！新版fastify推荐分开传，规避127.0.0.1回环bug
    await fastify.listen(PORT, '0.0.0.0')
    console.log("✅成功监听 0.0.0.0:" + PORT)
  } catch (err) {
    fastify.log.error(err)
    process.exit(1)
  }
}

start()

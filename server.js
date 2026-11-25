const fastify = require('fastify')({ logger: true })
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
    await fastify.listen({
      port: PORT,
      host: '0.0.0.0'
    })
    console.log("✅成功监听 0.0.0.0:" + PORT)
  } catch (err) {
    fastify.log.error(err)
    process.exit(1)
  }
}

start()

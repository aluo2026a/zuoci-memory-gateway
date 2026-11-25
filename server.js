const fastify = require('fastify')({ logger: true })

// 打印所有拿到的环境变量，用来调试
console.log("===== ENV DEBUG =====")
console.log("process.env.PORT =", process.env.PORT)
console.log("process.env.RAILWAY_PORT =", process.env.RAILWAY_PORT)

const PORT = parseInt(process.env.PORT || process.env.RAILWAY_PORT || 8080, 10)

fastify.get('/', async () => {
  return { msg: "Memory Gateway Alive ✅", portGot: PORT }
})

const start = async () => {
  try {
    await fastify.listen({ host: '0.0.0.0', port: PORT })
  } catch (err) {
    fastify.log.error(err)
    process.exit(1)
  }
}
start()


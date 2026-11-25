const fastify = require('fastify')({ logger: true })
const PORT = parseInt(process.env.PORT || 8080, 10)

fastify.get('/', async () => {
  return { msg: "Memory Gateway Alive ✅" }
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

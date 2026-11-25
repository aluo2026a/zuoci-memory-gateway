const fastify = require('fastify')({ logger: true })
const PORT = process.env.PORT || 8080

fastify.get('/', async () => {
  return { msg: "Memory Gateway Alive ✅" }
})

fastify.listen({ host: '0.0.0.0', port: PORT }, (err) => {
  if (err) {
    fastify.log.error(err)
    process.exit(1)
  }
  console.log(`Server listening on 0.0.0.0:${PORT}`)
})

import Fastify from 'fastify'
const fastify = Fastify({ logger: true })

fastify.get('/', async () => {
  return { msg: "记忆网关服务正常运行 ✅" }
})

const start = async () => {
  try {
    await fastify.listen({ port: process.env.PORT || 3000, host: '0.0.0.0' })
  } catch (err) {
    fastify.log.error(err)
    process.exit(1)
  }
}
start()

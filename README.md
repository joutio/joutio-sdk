# joutio-sdk

Agrégateur des librairies transverses Joutio : `joutio-parent`, `joutio-microservice`, `joutio-axon`.

## Modules

| Module                | Description                                                                       |
|-----------------------|-----------------------------------------------------------------------------------|
| `joutio-parent`       | Parent Maven des services et librairies Joutio (BOM, properties, plugins)         |
| `joutio-microservice` | Starter des microservices Joutio (CORS, Swagger, sécurité, WebSocket, exceptions) |
| `joutio-axon`         | Starter Axon distribué (RabbitMQ, deadlines, query bus)                           |

## Build

```bash
mvn -B verify
```

## Publish

Publié sur [GitHub Packages](https://github.com/orgs/joutio/packages) sous `com.joutio`.

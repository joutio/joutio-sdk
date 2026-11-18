# AGENTS.md — joutio-sdk

SDK **technique** Maven de tous les services : BOM, starters microservice et Axon distribué,
observabilité, console d'exploitation. Aucun code applicatif, **aucun contrat métier**
([règle](../../docs/backend/contrats-et-build.md#propriété-des-contrats)).

Lire : `../../AGENTS.md`, puis [`docs/backend/axon.md`](../../docs/backend/axon.md) et
[`contrats-et-build.md`](../../docs/backend/contrats-et-build.md).

## Modules

Package racine `com.joutio`. Une release publie tous les modules à la même version.

| Module | Rôle |
|---|---|
| `joutio-parent` | BOM parent (Spring Boot 3.5, Axon 4.13, Java 25, Lombok…), versions des artefacts du SDK, `build-info` hérité |
| `joutio-microservice-core` | types techniques partagés, dépendance des `*-domain` |
| `joutio-microservice-autoconfigure` | auto-configurations Spring |
| `joutio-microservice-starter` | point d'entrée des `*-infrastructure` (autoconfigure + `joutio-axon-starter`) |
| `joutio-axon-{starter,autoconfigure,deadline,query}` | Axon distribué : bus de commandes et de requêtes, transport Kafka, deadlines, codec `EventEnvelope` |
| `joutio-axon-test` | `JoutioAxonMatchers` pour `AggregateTestFixture` |

Les services déclarent `joutio-microservice-core` (domain) et `joutio-microservice-starter`
(infrastructure), sans version : le BOM la fixe.

## Contenu de `microservice-core`

- `BaseProblem`, `ProblemCategory` et les problèmes génériques (`SearchableProblems`,
  `LanguageProblems`…).
- `JoutioConstants` : identité du compte système (bot) `SYSTEM_USER_ID`, `SYSTEM_USER_EMAIL`,
  `SYSTEM_USER_NAME`.
- `Language` (`fr`/`en`, ISO 639-1, désérialisation stricte).
- Recherche : `SearchRequest`, `FilterRequest`, `SortRequest`, `PageRequest` → `SearchResponse<T>`
  (REST et bus), `@Searchable`, `FieldType`, `FilterOperator`.
- `EventEnvelope` : `aggregateId`, `sequenceNumber`, `timestamp`, `eventType`, `payload`. Le codec
  `EventEnvelopeModule` ne sert qu'au sérialiseur du bus de requêtes distribué et n'accepte que des
  types `com.joutio.` (fail-closed).
- `QueryResponseTypes` : seule fabrique des `ResponseType` (`instanceOf`, `optionalInstanceOf`,
  `multipleInstancesOf`, `searchResponseOf`). `multipleInstancesOf` règle le cas
  `List<EventEnvelope>` qu'Axon ne résout pas.
- `SecurityHelper` (autoconfigure) : `getUserId()`, `getPrincipal()`… dans les contrôleurs
  seulement.

## Auto-configurations

| Classe | Propriété | Rôle |
|---|---|---|
| `ResourceServerAutoConfiguration` | `microservice.resource-server.enabled` | JWT (issuer + JWK) ; `RestTemplate` OAuth2 `server-client` **`@Primary`** |
| `WebSocketAutoConfiguration` | `microservice.websocket.*` | STOMP/SockJS, JWT au `CONNECT`, heartbeats 10 s |
| `CorsAutoConfiguration`, `SwaggerAutoConfiguration`, `ActuatorAutoConfiguration`, `ExceptionAutoConfiguration` | `microservice.*.enabled` | CORS, OpenAPI, Actuator, handler RFC 7807 + intercepteurs Axon |
| `ObservabilityAutoConfiguration` + `ObservabilityEnvironmentPostProcessor` | `microservice.observability.*` | Prometheus, tags `application`/`environment`/`version`, logs JSON ECS, tracing OTLP (désactivé par défaut) |
| `AxonDistributed*AutoConfiguration` | `axon.distributed.*` (défauts du SDK) | bus de requêtes distribué, transport Kafka, enregistrement local, heartbeat, tracing et métriques Axon |
| `CommandGatewayRetryAutoConfiguration` | `joutio.axon.command-bus.max-retries` (3), `.retry-backoff` (100 ms) | `CommandGateway` qui réessaie les conflits de séquence et les échecs de routage, jamais un refus métier |
| `CapabilityDiscoveryAutoConfiguration` | `joutio.axon.command-bus.capabilities-retention` (2 min) | capacités des membres du bus stables malgré un échec HTTP (remplace l'ignore-listing d'Axon) |
| `TrackingProcessorDefaultsAutoConfiguration` | `joutio.axon.event-availability-timeout` (5 s) | réglages communs de tous les tracking processors ; token initial en tête pour les groupes `@Saga` |
| `InstanceScopedProcessingGroupsAutoConfiguration` | `joutio.axon.instance-scoped-groups` | un token par instance (`<groupe>-<HOSTNAME>`), démarrage en queue de flux |
| `SegmentedProcessingGroupsAutoConfiguration` + `SegmentRebalancer` | `joutio.axon.processors.<groupe>.segments`, `joutio.axon.rebalance-interval` (30 s) | groupe global segmenté, un thread par segment ; répartition des segments entre répliques découvertes |
| `AxonConsole*AutoConfiguration` | `joutio.axon.console.*`, `joutio.axon.dlq.*` | DLQ, état des processors, API interne, agent vers `joutio-console` |
| `AxonEventStoreErasureService` | — | effacement RGPD dans l'event store |
| `ByteaPostgreSQLDialect` | `spring.jpa.properties.hibernate.dialect` (défaut du SDK) | `@Lob byte[]` des entités Axon en `bytea` (schéma `V0`), pas en large objects |

Les défauts sont posés en priorité la plus basse. Un service ne redéclare **jamais**
`axon.axonserver.*`, `axon.kafka.*`, `axon.distributed.*` ni les propriétés d'observabilité : il
écraserait le défaut. Seules les surcharges ciblées sont permises.

## Console d'exploitation

- **DLQ active par défaut** : un événement en échec est mis de côté au lieu de bloquer le segment.
  Sont exclus :
  - les groupes instance-scopés ;
  - les sagas ;
  - les groupes de `joutio.axon.dlq.excluded-groups`.
- API interne `/internal/console/**`, jamais exposée par un ingress, protégée par un JWT M2M :
  - DLQ (lecture, retry, suppression) ;
  - pause/démarrage, reset de processor (à l'arrêt), split/merge ;
  - lecture de l'event store et des événements planifiés.
- Agent : `joutio.axon.console.enabled=true` pousse un instantané toutes les 15 s vers
  `joutio-console`.

## Commandes

```bash
mvn install              # avant tout build de service (le .m2 local doit contenir le SDK)
mvn verify
```

Release par `lib-release.yml`. Une évolution du SDK suit la chaîne :
1. release du SDK ;
2. bump de `joutio-parent` dans **chaque** service ;
3. release des services.

## Pièges

- **`RestTemplate` non `@Primary`** : sans `@Primary` sur le bean OAuth2, le bus de commandes
  d'axon-springcloud prend un `RestTemplate` sans jeton, et tout échoue en
  `No node known to accept command`. Ne jamais créer d'autre `RestTemplate` dans une
  auto-configuration partagée.
- **Processing groups** : chaque classe de handlers déclare `@ProcessingGroup` (kebab-case), sinon
  le démarrage échoue. Un replay ciblé consiste à supprimer la ligne `token_entry` du groupe (toutes
  les lignes suffixées pour un groupe instance-scopé).
- **Partitions** : `Axon.Events` est déclaré à 1 partition (`joutio.axon.kafka.partitions`). Chaque
  segment lit toutes les partitions : en ajouter ne parallélise rien. Le parallélisme passe par
  `joutio.axon.processors` ([axon](../../docs/backend/axon.md#segments-et-répliques)).
- **Anneau de routage** : le `Registration` de repli publie son `InstanceRegisteredEvent` à
  `ApplicationReadyEvent`. Sans lui, un pod se nomme `SERVICE[LOCAL]` dans son anneau et deux
  répliques traitent le même agrégat (verrouillé par `MultiReplicaRoutingTest`).
- **Ordre des auto-configurations** : ne jamais déclarer `after = SpringCloudAutoConfiguration` ; cet
  ordre fait passer le `RestTemplate` sans jeton d'Axon avant celui du SDK.
- **Configuration par nom d'un processor** : elle remplace entièrement le défaut. Toute
  configuration enregistrée par le SDK passe par `TrackingProcessorDefaults.apply`, sinon le groupe
  perd le timeout commun (et une saga son démarrage en tête de flux).
- **Segments** : ne jamais passer par `axon.eventhandling.processors.*`, qui rebrancherait le groupe
  sur l'event store local au lieu de Kafka.
- **Queue du flux Kafka** : elle s'obtient par `createHeadToken()`. `createTailToken()` renvoie `null`,
  donc le début du topic : un nouveau pod rejouerait tout.
- **Découverte** :
  - en local, `spring.cloud.discovery.client.simple.instances` ;
  - en prod, Kubernetes (`SPRING_CLOUD_KUBERNETES_*`, RBAC), filtré sur
    `app.kubernetes.io/component=axon` ;
  - l'adresse du `Registration` local doit correspondre à l'instance découverte : `server.address`,
    sinon IP du pod, sinon `localhost`. Ne pas la déduire de la présence d'un `SimpleDiscoveryClient`.
- `include-not-ready-addresses=true` : sans lui, un pod en démarrage n'est pas dans ses propres
  Endpoints et son seeder échoue (verrouillé par `SpringCloudCommandRouterLocalMemberTest`).
- `SimpleDiscoveryClient` n'émet pas de heartbeat : le SDK en ré-émet un toutes les 10 s pour
  rafraîchir les capacités.
- `enable-accept-all-commands=false` : chaque nœud n'annonce que ses propres handlers.
- Jauges et intercepteurs de métriques Axon : les enregistrer sur `ApplicationReadyEvent`, jamais
  pendant l'initialisation d'Axon (NPE, cycles de beans).
- Le format du bus de requêtes n'est pas rétrocompatible : déployer de façon coordonnée.
- Un compteur métier passe par un port du domaine (ex. `GameMetricsPort`), implémenté en
  infrastructure avec `MeterRegistry`. Jamais de Micrometer dans `domain/`.

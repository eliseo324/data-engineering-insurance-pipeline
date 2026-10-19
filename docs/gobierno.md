# Gobierno, seguridad y privacidad

El gobierno se declara como código y se reaplica en cada ejecución (tarea `gobierno` del job,
`notebooks/80_gobierno`):

- **Qué** se documenta y qué es dato personal: lo dicen los **contratos** (`conf/contracts/`).
- **Quién** accede a qué: lo dice `conf/gobierno.yml`.
- **Cómo** se protege, se retiene y se suprime: los módulos de `src/governance/`.

## 1. Objetos gobernados en Unity Catalog

Todo vive en `workspace.default` y lo gobierna Unity Catalog.

| Tipo | Objetos |
|---|---|
| Volumen | `volume1`: zona de aterrizaje `raw/<dataset>/` |
| Tablas Delta administradas | `bronze_*`, `silver_*`, `silver_cuarentena`, `gold_*`, `ctl_*` |
| Vistas | `vw_obs_*` (observabilidad), `vw_clientes_protegida`, `vw_cuarentena_protegida` |
| Funciones | `fn_es_lector_pii`, `fn_mask_documento`, `fn_mask_correo`, `fn_mask_telefono`, `fn_mask_fecha_nacimiento` |

**Documentación en el catálogo:**
- Cada tabla tiene un comentario, y cada columna de negocio y técnica tiene su descripción. Las de negocio salen del contrato; las técnicas, de `src/governance/catalogo.py`.
- Cada tabla lleva etiquetas: `capa`, `dominio`, `owner`, `contrato_version`, `contiene_pii` y `producto_datos`.
- Las columnas personales llevan además `pii` y `clasificacion`.

Con eso el catálogo se puede buscar ("¿qué tablas contienen datos personales?") y sirve de contexto para la IA.

## 2. Clasificación de datos personales

| Tabla y columna | `pii` | Clasificación | Tratamiento |
|---|---|---|---|
| clientes.numero_documento | alta | confidencial | Enmascarado: solo los últimos 4 dígitos |
| clientes.correo | alta | confidencial | Enmascarado: primera letra + dominio |
| clientes.telefono | alta | confidencial | Enmascarado: solo los últimos 3 dígitos |
| clientes.fecha_nacimiento | media | interna-sensible | Generalizado al 1 de enero del año (conserva el análisis por edad) |
| recaudos.referencia_bancaria | media | interna-sensible | Sin enmascarar: no identifica a la persona por sí sola. Se revisa con el área de seguridad |
| silver_cuarentena.registro (dataset clientes) | — | confidencial | Redactado en la vista protegida |
| bronze_clientes (todas las anteriores, en texto) | — | confidencial | Acceso solo para `ingenieria_datos` y `pii_lectores` |

**Seudonimización:** `cliente_id` es una clave seudónima. Gold no contiene datos personales: solo `cliente_id`, segmento y ciudad.

## 3. Roles y permisos (mínimo privilegio)

Los permisos se aplican como **estado deseado**:
- se revocan los privilegios previos de esquema y volumen de cada grupo gestionado;
- se otorgan exactamente los declarados;
- el plan es revisable sin ejecutarlo (`modo_gobierno = plan`).

| Rol (grupo) | Puede | No puede |
|---|---|---|
| `ingenieria_datos` | Leer y escribir el esquema, crear tablas y funciones, leer y escribir el volumen | — (en producción el job corre con un *service principal* de este grupo) |
| `analistas_negocio` | Leer `gold_poliza_360`, `gold_siniestralidad_mensual`, `gold_discrepancias` y `vw_clientes_protegida` | Ver Bronze, Silver ni datos personales en claro |
| `auditores_calidad` | Leer `gold_calidad_pipeline`, `ctl_*`, `vw_obs_*` y `vw_cuarentena_protegida` | Ver datos personales |
| `pii_lectores` | Leer `silver_clientes`, `silver_cuarentena` y ver en claro las vistas protegidas | Modificar datos |
| `evaluadores` | Leer todas las tablas y vistas **excepto** las de datos personales en claro; ejecutar funciones; leer el volumen (excepción temporal documentada) | Ver documento, correo, teléfono ni fecha de nacimiento en claro |

**Controles en el código:** el plan de permisos **rechaza** cualquier configuración que dé acceso directo a una tabla con datos personales a un rol distinto de `ingenieria_datos` o `pii_lectores`. Las pruebas automatizadas lo verifican.

Para acceder a objetos del workspace (job, notebooks, tablero, SQL warehouse), los permisos se otorgan con *Can view* o *Can use*. `notebooks/90_desplegar_job` asigna `CAN_VIEW` sobre el job a los evaluadores.

## 4. Enmascaramiento

- **Mecanismo por defecto:** funciones de Unity Catalog + vistas protegidas. Las funciones devuelven el valor en claro solo si `is_account_group_member('pii_lectores')`; en otro caso lo enmascaran o generalizan. Los consumidores reciben permiso sobre las vistas, nunca sobre las tablas base.
- **Opcional:** *column masks* nativas (`ALTER TABLE … ALTER COLUMN … SET MASK fn_mask_*`), activables con `aplicar_column_masks = true`. Por defecto están desactivadas, por tres razones:
  1. en Free Edition no hay consola de cuenta para gestionar grupos de cuenta; si el grupo no existe, la máscara se aplicaría incluso al propio pipeline;
  2. las máscaras sobre tablas que el pipeline modifica con `MERGE` deben validarse antes en el entorno corporativo;
  3. las vistas protegidas cumplen el mismo objetivo sin afectar las escrituras.

  En el entorno corporativo se activan una vez los grupos de cuenta están sincronizados por SCIM.

## 5. Linaje y trazabilidad

| Nivel | Cómo |
|---|---|
| Tabla y columna | Unity Catalog captura el linaje automáticamente para notebooks y jobs. Se ve en *Catalog Explorer → tabla → Lineage*, por ejemplo `bronze_polizas → silver_polizas → gold_poliza_360` |
| Registro | Cada fila conserva `_source_file`, `_source_file_hash`, `_row_hash`, `_bronze_run_id` y `_silver_run_id`. `notebooks/31_preguntas_negocio` traza una póliza de Gold hasta su archivo y su ejecución |
| Ejecución | `ctl_ejecuciones` (quién, cuándo, qué tarea, cuántos registros) y `ctl_publicaciones` (versión Delta publicada) |

## 6. Auditoría

- **Operación:** `ctl_ejecuciones.usuario`, `ctl_archivos_procesados`, `ctl_calidad_resultados`, `ctl_alertas` y `DESCRIBE HISTORY` de cada tabla Delta (quién modificó qué versión y con qué operación).
- **Entorno corporativo:** además, `system.access.audit`, que registra accesos y cambios de permisos de Unity Catalog, y `system.access.table_lineage`, con retención y alertas sobre accesos a tablas `contiene_pii = true`. Free Edition no expone estas *system tables*.

## 7. Ownership y documentación

- **Responsable del contenido:** cada dataset tiene un dueño de negocio (`owner` en el contrato y etiqueta `owner` en la tabla), que responde por las reglas y la corrección en origen. El equipo de ingeniería es responsable técnico de las capas y de los productos Gold.
- **Propiedad de los objetos:** en el entorno corporativo los objetos pertenecen a grupos o *service principals* (`ALTER TABLE … OWNER TO`), nunca a personas. En Free Edition el dueño técnico es el usuario del workspace.
- **Documentación funcional:** `docs/productos_gold.md` (productos), `docs/decisiones.md` (supuestos y decisiones) y los contratos (esquema y semántica).

## 8. Secretos y credenciales

- **El repositorio no contiene credenciales:**
    - el SDK se autentica con la identidad del usuario del notebook o del job;
    - las fuentes son archivos del volumen;
    - la prueba `tests/test_sin_secretos.py` busca tokens, llaves y contraseñas, y falla la CI si encuentra alguno;
    - `.gitignore` excluye `.env`, `.databrickscfg`, llaves y datos.
- **Entorno corporativo:**
    - secretos en Azure Key Vault expuestos como *secret scopes* (`dbutils.secrets.get("kv-seguros", "jdbc-core-polizas")`);
    - en CI/CD, *OIDC federation* de GitHub o Azure DevOps con *service principals* (sin tokens de larga vida);
    - *secret scanning* activado en el repositorio.

| Zona | Retención | Mecanismo |
|---|---|---|
| Volumen `raw/` (archivos fuente) | 90 días después de la ingesta | Archivado o borrado programado. Bronze ya conserva la copia fiel con su hash |
| Bronze y Silver con datos personales | Mientras exista relación contractual y deber legal | `delta.deletedFileRetentionDuration = 7 días` y `delta.logRetentionDuration = 30 días` limitan el *time travel* sobre datos personales |
| Gold (sin datos personales) | Según la retención documental regulatoria | Snapshots mensuales por `fecha_corte` |
| Tablas de control | 2 años | Depuración periódica por fecha |

**Derecho de supresión** (`notebooks/81_derecho_supresion`):
1. Se reciben y validan la solicitud y el `cliente_id`. El identificador se valida contra un patrón, lo que evita inyección SQL.
2. Se suprimen los atributos personales en `bronze_clientes`, `silver_clientes` y la cuarentena, conservando la clave seudónima. Así las pólizas y siniestros que deben conservarse siguen íntegros, sin identificar a la persona.
3. La solicitud queda registrada en `ctl_ejecuciones` solo con el hash del identificador.
4. Pasada la retención de versiones (7 días), se ejecuta `VACUUM` para la eliminación física.
5. **Limitación conocida (DT-05):** los archivos de `raw/` que aún estén dentro de su retención contienen el dato original. Un reproceso completo lo reintroduciría. La solución corporativa es una tabla de supresiones consultada por Silver y el borrado o reescritura del archivo en la zona de aterrizaje.

## 10. Equivalente corporativo

| Free Edition (implementado) | Corporativo (propuesto) |
|---|---|
| Grupos locales creados por SCIM desde el notebook | Grupos de Entra ID sincronizados a la cuenta por SCIM |
| Vistas protegidas + funciones de máscara | Column masks y row filters nativos + vistas protegidas para consumo externo |
| Etiquetas aplicadas por el pipeline | Etiquetas gobernadas (*governed tags*) con políticas basadas en atributos (ABAC) |
| Auditoría con tablas `ctl_*` e historial Delta | `system.access.audit`, alertas de acceso a PII e integración con el SIEM |
| Dueño técnico: usuario | Dueños: grupos y *service principals*; un catálogo por ambiente |

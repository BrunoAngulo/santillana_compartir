# Extension del navegador

La carpeta [browser_extension/santillana_session_helper](./santillana_session_helper) contiene una extension MV3 para Chrome/Edge que lee:

- `pgs-jwt-token` desde `https://apps.santillanacompartir.com/`
- `accessToken` desde `https://richmondstudio.global/`
- `_session_id` y el `cookie header` completo desde `https://loqueleodigital.com/`
- `local-santadmin` para IPA desde dominios Santillana permitidos por la extension

## Instalar

1. Abre `chrome://extensions` o `edge://extensions`
2. Activa `Modo desarrollador`
3. Pulsa `Cargar descomprimida`
4. Selecciona la carpeta `browser_extension/santillana_session_helper`

## Usar

1. Haz clic en la extension
2. Pulsa `Leer datos`
3. Copia el valor que necesites

La extension intenta reutilizar una pestaña ya abierta del dominio. Si no existe, abre una pestaña temporal en segundo plano, lee el valor y la cierra.

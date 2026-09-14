# T-Bank sandbox TLS trust

`russian-trusted-root-ca.pem` — публичный сертификат Russian Trusted Root CA. Он нужен для проверки сертификата официального `sandbox-invest-public-api.tbank.ru`. Приватного ключа в файле нет.

Требование описано в [документации Т-Банка о подключении](https://developer.tbank.ru/invest/intro/developer/network). Файл получен из [официального SDK Т-Банка](https://opensource.tbank.ru/api/v4/projects/238/repository/files/t_tech%2Finvest%2Fcerts%2FRussianTrustedRootCA.pem/raw?ref=b07a28adb2ab6a0e3543d629b4b6cec58ddd6aeb). DER-отпечаток совпадает с [сертификатом, распространяемым Госуслугами](https://gu-st.ru/content/Other/doc/russian_trusted_root_ca.cer).

- SHA256 файла SDK (CRLF PEM): `e4370c9b6b540f063ba1829222d2d6041cbb0bfc5d001ee6bbb97620914594dc`.
- В репозитории переводы строк нормализованы в LF; сам сертификат и DER-отпечаток сохранены.
- SHA256 DER: `d26d2d0231b7c39f92cc738512ba54103519e4405d68b5bd703e9788ca8ecf31`.
- Срок действия: 2022-03-01 — 2032-02-27.

Workflow и команды `npm run study` / `npm run observe:market` передают файл через `GRPC_DEFAULT_SSL_ROOTS_FILE_PATH` только процессам работы с брокером. Это не устанавливает сертификат в систему. Проверка цепочки сертификатов и имени сервера остаётся включённой. Отключение проверки TLS и подмена имени сервера не используются. При прямом запуске compiled JS нужно установить эту переменную до запуска Node.js.

Сертификат хранится в репозитории, проверяется тестом по DER-отпечатку и включается в хеши происхождения рыночной записи. Во время планового запуска скачивание новых сертификатов не выполняется.

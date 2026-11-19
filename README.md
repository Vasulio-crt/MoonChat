# Лунный чат запуск
Лучший лунный чатик. Это по факту простой глобальный чат для друзей 

## Первое что сделать
Это собрать докер образ
```bash
docker build -t moon-chat .
```

## Второе что сделать
Запустить этот докер образ
```bash
docker run --name moon-chat -p 7000:8080 -v ./data:/app/data moon-chat:latest
```

Открой **локально** - http://localhost:7000

### Запустить в фоне
```bash
docker run --name moon-chat -p 7000:8080 -v ./data:/app/data -d moon-chat:latest
```
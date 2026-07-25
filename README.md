# Algoritmos Distribuidos

Implementaciones en Rust de algoritmos de elección de líder para la materia **Programación Concurrente** (UBA FIUBA).

## Algoritmos incluidos

| Archivo | Algoritmo | Referencia |
|---------|-----------|------------|
| `bully.rs` | Bully (elección por jerarquía) | Garcia-Molina, 1982 |
| `ring.rs` | Ring / Chang-Roberts (elección en anillo) | Chang & Roberts, 1979 |

Ambos simulan 5 procesos (P1..P5) comunicándose exclusivamente por mensajes (`mpsc::channel`), sin memoria compartida mutable. Cada escenario incluye la inyección de una falla crash-stop del líder para demostrar la convergencia del protocolo.

## Compilación y ejecución

No requiere dependencias externas (solo `std`).

```bash
rustc bully.rs -o bully && ./bully
rustc ring.rs -o ring && ./ring
```

O con Cargo (requiere mover los archivos a `src/bin/` o configurar `[[bin]]` en `Cargo.toml`).

## Diseño

- Cada nodo es un hilo de OS (`std::thread`).
- Cada enlace de red es un canal `mpsc`, modelando comunicación por mensajes (sin memoria compartida).
- Las fallas se inyectan vía `Arc<AtomicBool>` compartido con el orquestador (`main`), simulando un crash-stop sin matar realmente el hilo.

// =============================================================================
// Algoritmo de Ring / Chang-Roberts (1979) - Caso de uso
// Programación Concurrente - UBA FIUBA
// =============================================================================
//
// Este programa simula 5 procesos (P1..P5) organizados en un anillo logico
// unidireccional: P1 -> P2 -> P3 -> P4 -> P5 -> (vuelta a P1). Cada proceso
// es un hilo independiente y cada enlace se modela con un canal std::sync::mpsc.
//
// A diferencia de Bully (grafo completo, cada nodo conoce a todos), en Ring
// cada nodo solo necesita conocer a su sucesor logico. En esta simulacion esa
// responsabilidad se modela con una estructura `Network`, que representa la
// capa de red: conoce el orden del anillo y el estado vivo/caido de cada
// nodo, y es quien resuelve el "bypass" (salto) de los nodos caidos, tal
// como lo hace la capa de transporte/heartbeat en un sistema real.
//
// Escenario simulado:
//   1. Arrancan 5 nodos en anillo. No hay lider inicial explicito (no hace
//      falta para demostrar el algoritmo de eleccion en si).
//   2. El "main" simula la caida (crash-stop) de P5.
//   3. El "main" dispara la deteccion de falla en P2 (en un sistema real
//      este disparo saldria de un mecanismo de heartbeat/timeout, igual al
//      implementado en el caso de uso de Bully; aqui se inyecta
//      explicitamente para mantener el ejemplo enfocado en el mecanismo
//      distintivo de Ring: la acumulacion de IDs y el bypass).
//   4. El mensaje de ELECCION recorre el anillo acumulando los IDs vivos y
//      saltando a P5 (caido). Al volver a P2, se calcula el maximo (P4) y
//      se propaga un segundo mensaje COORDINATOR que da una vuelta completa
//      para anunciar el resultado a todos.
//
// Compilación (no requiere dependencias externas, solo std):
//   rustc ring.rs -o ring
//   ./ring
//
// =============================================================================

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

/// Mensajes del protocolo de Ring (Chang-Roberts).
#[derive(Debug, Clone)]
enum Message {
    /// Mensaje de eleccion: acumula los IDs de los nodos vivos que atraveso.
    Election(Vec<u32>),
    /// Anuncio del lider electo. `origin` es quien inicio la eleccion y por
    /// lo tanto quien debe detener la propagacion cuando el mensaje vuelve.
    Coordinator { leader: u32, origin: u32 },
    /// Senal inyectada externamente (por "main") para forzar el inicio de
    /// una eleccion, emulando la deteccion de falla del lider.
    StartElection,
}

/// Representa la capa de red del anillo: conoce el orden logico de los
/// procesos y el estado vivo/caido de cada uno, y resuelve el salto
/// (bypass) sobre los nodos caidos para mantener la continuidad del anillo.
struct Network {
    order: Vec<u32>,
    senders: HashMap<u32, Sender<Message>>,
    alive: HashMap<u32, Arc<AtomicBool>>,
}

impl Network {
    /// Busca, a partir de `from`, el proximo nodo vivo siguiendo el orden
    /// del anillo, saltando automaticamente los nodos caidos.
    fn next_alive(&self, from: u32) -> Option<u32> {
        let n = self.order.len();
        let start = self.order.iter().position(|&x| x == from)?;

        for offset in 1..=n {
            let idx = (start + offset) % n;
            let candidate = self.order[idx];
            if candidate == from {
                // Dimos toda la vuelta: no hay ningun otro nodo vivo.
                return None;
            }
            let is_alive = self
                .alive
                .get(&candidate)
                .map(|a| a.load(Ordering::SeqCst))
                .unwrap_or(false);
            if is_alive {
                return Some(candidate);
            }
        }
        None
    }

    fn forward(&self, from: u32, msg: Message) {
        match self.next_alive(from) {
            Some(target) => {
                if let Some(tx) = self.senders.get(&target) {
                    let _ = tx.send(msg);
                }
            }
            None => {
                println!("[red] P{} no tiene sucesor vivo; mensaje descartado.", from);
            }
        }
    }
}

struct Node {
    id: u32,
    rx: Receiver<Message>,
    network: Arc<Network>,
}

impl Node {
    fn run(self) {
        loop {
            match self.rx.recv() {
                Ok(Message::StartElection) => self.start_election(),
                Ok(Message::Election(ids)) => self.handle_election(ids),
                Ok(Message::Coordinator { leader, origin }) => {
                    self.handle_coordinator(leader, origin)
                }
                Err(_) => break, // todos los Sender fueron liberados
            }
        }
    }

    fn start_election(&self) {
        println!("[P{}] Detecto fallo del lider. Inicio ELECCION (RING).", self.id);
        self.network.forward(self.id, Message::Election(vec![self.id]));
    }

    fn handle_election(&self, mut ids: Vec<u32>) {
        if ids[0] == self.id {
            // El mensaje completo la vuelta: soy quien inicio la eleccion.
            let leader = *ids.iter().max().unwrap();
            println!(
                "[P{}] La vuelta se completo. IDs vivos recolectados: {:?}. Lider electo: P{}.",
                self.id, ids, leader
            );
            self.network.forward(
                self.id,
                Message::Coordinator {
                    leader,
                    origin: self.id,
                },
            );
        } else {
            println!("[P{}] Recibo ELECTION {:?}; agrego mi ID y reenvio.", self.id, ids);
            ids.push(self.id);
            self.network.forward(self.id, Message::Election(ids));
        }
    }

    fn handle_coordinator(&self, leader: u32, origin: u32) {
        println!("[P{}] Recibo COORDINATOR: el nuevo lider es P{}.", self.id, leader);
        if self.id != origin {
            self.network
                .forward(self.id, Message::Coordinator { leader, origin });
        } else {
            println!("[P{}] El anuncio completo la vuelta. Eleccion finalizada.", self.id);
        }
    }
}

fn main() {
    let order: Vec<u32> = vec![1, 2, 3, 4, 5];

    let mut senders: HashMap<u32, Sender<Message>> = HashMap::new();
    let mut receivers: HashMap<u32, Receiver<Message>> = HashMap::new();
    for &id in &order {
        let (tx, rx) = mpsc::channel();
        senders.insert(id, tx);
        receivers.insert(id, rx);
    }

    let alive: HashMap<u32, Arc<AtomicBool>> = order
        .iter()
        .map(|&id| (id, Arc::new(AtomicBool::new(true))))
        .collect();

    let network = Arc::new(Network {
        order: order.clone(),
        senders,
        alive,
    });

    for &id in &order {
        let rx = receivers.remove(&id).unwrap();
        let net = Arc::clone(&network);
        thread::spawn(move || {
            Node { id, rx, network: net }.run();
        });
    }

    println!("[main] Anillo inicial: P1 -> P2 -> P3 -> P4 -> P5 -> (vuelta a P1)");
    thread::sleep(Duration::from_millis(200));

    println!("[main] *** Simulando la caida de P5 ***");
    network.alive.get(&5).unwrap().store(false, Ordering::SeqCst);
    thread::sleep(Duration::from_millis(200));

    println!("[main] *** P2 detecta la falla e inicia una ELECCION ***");
    network
        .senders
        .get(&2)
        .unwrap()
        .send(Message::StartElection)
        .unwrap();

    thread::sleep(Duration::from_millis(500));
    println!("[main] Fin de la simulacion.");
}

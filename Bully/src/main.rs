// =============================================================================
// Algoritmo de Bully (Garcia-Molina, 1982) - Caso de uso
// Programación Concurrente - UBA FIUBA
// =============================================================================
//
// Este programa simula un clúster de 5 procesos (P1..P5) que ejecutan el
// algoritmo de Bully para elegir un líder. Cada proceso se modela como un
// hilo de sistema operativo (std::thread) independiente, y cada enlace de
// red se modela con un canal std::sync::mpsc, respetando el modelo real:
// los nodos NO comparten memoria mutable, solo intercambian mensajes.
//
// Escenario simulado:
//   1. Arrancan 5 nodos (IDs 1 a 5). P5 es el líder inicial.
//   2. El "main" (que actúa como orquestador externo / capa de fallas)
//      simula la caída (crash-stop) del líder P5.
//   3. Los seguidores detectan, vía timeout de heartbeat, que el líder dejó
//      de responder, e inician el proceso de elección según el protocolo
//      de Bully (mensajes ELECTION / OK / COORDINATOR).
//   4. El proceso converge de forma determinista al nuevo líder: P4 (el
//      identificador más alto entre los procesos vivos), sin importar el
//      orden exacto en que los seguidores detecten la falla.
//
// Compilación (no requiere dependencias externas, solo std):
//   rustc bully.rs -o bully
//   ./bully
//
// =============================================================================

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

/// Mensajes del protocolo de Bully.
#[derive(Debug, Clone, Copy)]
enum Message {
    /// Un proceso candidato le pregunta a los de mayor jerarquía si siguen vivos.
    Election { from: u32 },
    /// Respuesta de un proceso de mayor jerarquía: "estoy vivo, retirate".
    Ok { from: u32 },
    /// Anuncio del nuevo líder a todo el clúster.
    Coordinator { leader: u32 },
    /// Latido periódico que el líder emite para que los seguidores sepan
    /// que sigue con vida.
    Heartbeat { from: u32 },
}

/// Umbral de tiempo sin heartbeat del líder antes de sospechar una falla.
const HEARTBEAT_TIMEOUT: Duration = Duration::from_millis(350);
/// Tiempo máximo que un candidato espera un OK antes de autoproclamarse.
const ELECTION_TIMEOUT: Duration = Duration::from_millis(250);
/// Intervalo de polling de cada nodo sobre su canal de entrada.
const POLL_INTERVAL: Duration = Duration::from_millis(80);

struct Node {
    id: u32,
    /// Topología lógica de grafo completo: un emisor hacia cada par conocido.
    peers: HashMap<u32, Sender<Message>>,
    rx: Receiver<Message>,
    /// Bandera compartida con "main": permite inyectar una falla crash-stop
    /// sin necesidad de matar realmente el hilo.
    alive: Arc<AtomicBool>,

    current_leader: Option<u32>,
    awaiting_ok: bool,
    election_start: Option<Instant>,
    last_heartbeat: Instant,
}

impl Node {
    fn new(
        id: u32,
        peers: HashMap<u32, Sender<Message>>,
        rx: Receiver<Message>,
        alive: Arc<AtomicBool>,
        initial_leader: Option<u32>,
    ) -> Self {
        Node {
            id,
            peers,
            rx,
            alive,
            current_leader: initial_leader,
            awaiting_ok: false,
            election_start: None,
            last_heartbeat: Instant::now(),
        }
    }

    fn send_to(&self, target: u32, msg: Message) {
        if let Some(tx) = self.peers.get(&target) {
            let _ = tx.send(msg);
        }
    }

    fn broadcast(&self, msg: Message) {
        for (&pid, tx) in self.peers.iter() {
            if pid != self.id {
                let _ = tx.send(msg);
            }
        }
    }

    /// Inicia una elección: envía ELECTION a todos los procesos de ID mayor.
    /// Si no hay ninguno (soy el de mayor ID entre los vivos que conozco),
    /// me autoproclamo de inmediato.
    fn start_election(&mut self) {
        println!("[P{}] No recibo heartbeat del lider. Inicio ELECCION.", self.id);
        let higher: Vec<u32> = self
            .peers
            .keys()
            .copied()
            .filter(|&pid| pid > self.id)
            .collect();

        if higher.is_empty() {
            self.become_leader();
        } else {
            for pid in higher {
                self.send_to(pid, Message::Election { from: self.id });
            }
            self.awaiting_ok = true;
            self.election_start = Some(Instant::now());
        }
    }

    fn become_leader(&mut self) {
        self.current_leader = Some(self.id);
        self.awaiting_ok = false;
        self.election_start = None;
        self.last_heartbeat = Instant::now();
        println!(
            "[P{}] >>> Nadie de mayor jerarquia respondio. Me autoproclamo LIDER. <<<",
            self.id
        );
        self.broadcast(Message::Coordinator { leader: self.id });
    }

    fn handle_message(&mut self, msg: Message) {
        match msg {
            Message::Election { from } => {
                println!("[P{}] Recibo ELECTION de P{}.", self.id, from);
                // Solo respondo si tengo mayor jerarquia que quien pregunta.
                if self.id > from {
                    self.send_to(from, Message::Ok { from: self.id });

                    if self.current_leader == Some(self.id) {
                        // Ya soy el lider: se lo confirmo directamente en
                        // lugar de esperar a que reciba un COORDINATOR viejo.
                        self.send_to(from, Message::Coordinator { leader: self.id });
                    } else if !self.awaiting_ok {
                        // Un ELECTION de un ID menor sugiere que el lider
                        // podria estar caido: yo tambien inicio candidatura.
                        self.start_election();
                    }
                }
            }
            Message::Ok { from } => {
                println!("[P{}] Recibo OK de P{}; cedo la candidatura.", self.id, from);
                self.awaiting_ok = false;
                self.election_start = None;
            }
            Message::Coordinator { leader } => {
                self.current_leader = Some(leader);
                self.last_heartbeat = Instant::now();
                self.awaiting_ok = false;
                self.election_start = None;
                println!("[P{}] Recibo COORDINATOR: el nuevo lider es P{}.", self.id, leader);
            }
            Message::Heartbeat { from } => {
                self.current_leader = Some(from);
                self.last_heartbeat = Instant::now();
            }
        }
    }

    fn run(mut self) {
        loop {
            // Fallo crash-stop inyectado por "main": el nodo deja de
            // procesar (ni lee ni envia), simulando una caida real.
            if !self.alive.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_millis(50));
                continue;
            }

            match self.rx.recv_timeout(POLL_INTERVAL) {
                Ok(msg) => self.handle_message(msg),
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    if self.current_leader == Some(self.id) {
                        // Soy el lider: emito heartbeat.
                        self.broadcast(Message::Heartbeat { from: self.id });
                    } else if self.last_heartbeat.elapsed() > HEARTBEAT_TIMEOUT
                        && !self.awaiting_ok
                    {
                        self.start_election();
                    }

                    if self.awaiting_ok {
                        if let Some(t) = self.election_start {
                            if t.elapsed() > ELECTION_TIMEOUT {
                                self.become_leader();
                            }
                        }
                    }
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
    }
}

fn main() {
    let ids: Vec<u32> = vec![1, 2, 3, 4, 5];

    let mut senders: HashMap<u32, Sender<Message>> = HashMap::new();
    let mut receivers: HashMap<u32, Receiver<Message>> = HashMap::new();
    for &id in &ids {
        let (tx, rx) = mpsc::channel();
        senders.insert(id, tx);
        receivers.insert(id, rx);
    }

    let alive_flags: HashMap<u32, Arc<AtomicBool>> = ids
        .iter()
        .map(|&id| (id, Arc::new(AtomicBool::new(true))))
        .collect();

    for &id in &ids {
        let rx = receivers.remove(&id).unwrap();
        let peers = senders.clone();
        let alive = alive_flags.get(&id).unwrap().clone();
        // P5 arranca como lider conocido; el resto arranca sin lider
        // conocido y lo aprende al recibir el primer heartbeat.
        let initial_leader = if id == 5 { Some(5) } else { None };

        thread::spawn(move || {
            Node::new(id, peers, rx, alive, initial_leader).run();
        });
    }

    println!("[main] Sistema iniciado con 5 nodos. Lider inicial: P5.");
    thread::sleep(Duration::from_millis(600));

    println!("[main] *** Simulando la caida del lider P5 (crash-stop) ***");
    alive_flags.get(&5).unwrap().store(false, Ordering::SeqCst);

    thread::sleep(Duration::from_millis(1500));
    println!("[main] Fin de la simulacion. Lider final esperado: P4.");
}

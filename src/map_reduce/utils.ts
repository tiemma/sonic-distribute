import { cpus } from "os";
import cluster, { ClusterSettings } from "cluster";
import { pid } from "process";
import { Queue } from "@tiemma/sonic-core";

export type MasterFn = (workerQueue: Queue, args: any) => any;
export type WorkerFn = (event: MapReduceEvent, args: any) => any;
export type ReduceFn = (resultQueue: Queue) => any;

export const NUM_CPUS = cpus().length;

export const isMaster = () => {
  return cluster.isMaster || cluster.isPrimary;
};

export const getWorkerName = () => {
  if (isMaster()) {
    return "MASTER";
  }

  return `WORKER-${cluster.worker.id}`;
};

export const Delay = (time = Math.random() * 50) =>
  new Promise((resolve) => setTimeout(resolve, time));
export const getLogger =
  (loggerID: string) =>
  (message: any, date = new Date().toISOString()) => {
    if (process.env["QUIET"]) return;
    // eslint-disable-next-line no-console
    console.log(`${date}: ${loggerID}: ${message}`);
  };
const logger = getLogger(getWorkerName());

export const clusterEvents = {
  MESSAGE: "message",
  DISCONNECT: "disconnect",
};

export const getWorkerID = async (workerQueue: Queue) => {
  while (workerQueue.isEmpty()) {
    await Delay();
  }

  let workerID = workerQueue.dequeue();
  while (!cluster.workers[workerID]) {
    workerID = workerQueue.dequeue();
  }

  return workerID;
};

export interface MapReduceEvent {
  data: any;

  //  Internal, not to be directly used
  id?: number;
  SYN?: boolean;
  ACK?: boolean;
  SYN_ACK?: boolean;
}

export const configureWorkers = async (numWorkers: number) => {
  const workerQueue = new Queue();
  const processOrder = new Queue();

  for (let i = 0; i < numWorkers; i++) {
    const worker = cluster.fork();

    worker.on(clusterEvents.MESSAGE, (message: MapReduceEvent) => {
      const { id, SYN, SYN_ACK, data } = message;

      logger(`Received events from worker: ${JSON.stringify(message)}`);

      if (SYN) {
        // Return signal to worker to start processing
        worker.send({ ACK: true });
      } else if (SYN_ACK || id) {
        workerQueue.enqueue(worker.id);
        logger(`Worker ${worker.id} now available`);
      }

      if (data) {
        processOrder.enqueue(message);
      }
    });

    worker.on(clusterEvents.DISCONNECT, () => {
      logger(`Gracefully shutting down worker #${worker.id}`);
    });
  }

  logger("Worker queues initializing");
  while (workerQueue.getElements().length !== numWorkers) {
    await Delay();
  }
  logger(`Workers queue populated`);

  return { workerQueue, processOrder };
};

export const initMaster = async (args: any) => {
  (cluster.setupMaster || cluster.setupPrimary)({
    execArgv: [
      "-r",
      "tsconfig-paths/register",
      "-r",
      "ts-node/register",
      "--async-stack-traces",
    ],
  } as ClusterSettings);

  logger("Running Map reduce");
  logger(`Process running on pid ${pid}`);

  const { numWorkers = NUM_CPUS } = args;

  return configureWorkers(numWorkers);
};

export const initWorkers = async (workerFn: any, args: any) => {
  const logger = getLogger(getWorkerName());

  // Register worker on startup
  process.send({ SYN: true });

  process.on(clusterEvents.MESSAGE, async (event) => {
    // Establish master-node communication with 3-way handshake
    if (event.ACK) {
      logger(`Worker ${cluster.worker.id} now active and processing requests`);

      process.send({ SYN_ACK: true });

      return;
    }

    const data = await workerFn(event, {
      ...args,
      workerID: cluster.worker.id,
    });
    const res = { id: cluster.worker.id, data };
    logger(`Writing event ${JSON.stringify(res)} to master`);

    process.send(res);
  });
};

export const shutdown = async () => {
  for (let i = 0; i < NUM_CPUS; i++) {
    if (cluster.workers[i]) {
      await Delay(100);
      cluster.workers[i].disconnect();
    }
  }

  logger(`Shutting down master`);
};

export const Map = async (workerQueue: Queue, event: MapReduceEvent) => {
  const workerID = await getWorkerID(workerQueue);

  logger(`Sending event ${JSON.stringify(event)} to worker ${workerID}`);

  cluster.workers[workerID].send(event);
};

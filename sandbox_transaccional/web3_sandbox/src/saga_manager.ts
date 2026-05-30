/**
 * @module saga_manager
 * 
 * Saga Transaction Manager for Cross-Market Operations.
 * Orchestrates distributed transactions spanning Web3 (Polymarket CLOB)
 * and TradFi (Alpaca / IBKR) venues. Enforces automatic compensations (reversals)
 * on failures to eliminate unhedged delta exposure.
 */

import { randomUUID } from 'crypto';

export type StepStatus = 'PENDING' | 'EXECUTING' | 'SUCCESS' | 'FAILED' | 'COMPENSATING' | 'COMPENSATED';
export type SagaStatus = 'PENDING' | 'EXECUTING' | 'COMPLETED' | 'COMPENSATING' | 'COMPENSATED' | 'FAILED';

export interface SagaStep {
  name: string;
  execute: () => Promise<boolean>;
  compensate: () => Promise<boolean>;
  status: StepStatus;
  error?: string;
}

export interface SagaResult {
  sagaId: string;
  status: SagaStatus;
  executedStepsCount: number;
  compensatedStepsCount: number;
  timeline: Array<{ event: string; timestamp: string }>;
  error?: string;
}

export class SagaOrchestrator {
  private timeline: Array<{ event: string; timestamp: string }> = [];

  constructor() {
    console.log('SagaOrchestrator instantiated.');
  }

  private logEvent(sagaId: string, event: string): void {
    const timestamp = new Date().toISOString();
    this.timeline.push({ event, timestamp });
    console.log(`[SAGA][${sagaId}] ${event}`);
  }

  /**
   * executeSaga
   * 
   * Runs the chain of steps sequentially. If any step returns false or throws,
   * it rolls back completed steps in reverse order by running their compensations.
   */
  public async executeSaga(steps: SagaStep[]): Promise<SagaResult> {
    const sagaId = randomUUID();
    this.logEvent(sagaId, `Saga transaction execution started. Total steps: ${steps.length}`);
    
    let currentStepIndex = 0;
    let success = true;
    let sagaError: string | undefined;

    for (let i = 0; i < steps.length; i++) {
      const step = steps[i]!;
      step.status = 'EXECUTING';
      this.logEvent(sagaId, `Executing step: ${step.name}`);

      try {
        const stepSuccess = await step.execute();
        if (stepSuccess) {
          step.status = 'SUCCESS';
          this.logEvent(sagaId, `Step succeeded: ${step.name}`);
          currentStepIndex = i;
        } else {
          step.status = 'FAILED';
          step.error = 'Execution returned false';
          this.logEvent(sagaId, `Step failed: ${step.name}`);
          success = false;
          sagaError = `Step ${step.name} failed.`;
          break;
        }
      } catch (e: any) {
        step.status = 'FAILED';
        step.error = e.message;
        this.logEvent(sagaId, `Step failed with exception: ${step.name} (${e.message})`);
        success = false;
        sagaError = e.message;
        break;
      }
    }

    if (success) {
      this.logEvent(sagaId, 'Saga completed successfully.');
      return {
        sagaId,
        status: 'COMPLETED',
        executedStepsCount: steps.length,
        compensatedStepsCount: 0,
        timeline: this.timeline,
      };
    }

    // Rollback phase (Compensations)
    this.logEvent(sagaId, 'Executing compensations (rollback)...');
    let compensatedStepsCount = 0;

    for (let i = currentStepIndex; i >= 0; i--) {
      const step = steps[i]!;
      step.status = 'COMPENSATING';
      this.logEvent(sagaId, `Compensating step: ${step.name}`);

      try {
        const compSuccess = await step.compensate();
        if (compSuccess) {
          step.status = 'COMPENSATED';
          this.logEvent(sagaId, `Step compensated: ${step.name}`);
          compensatedStepsCount++;
        } else {
          console.error(`[CRITICAL] Compensation failed for step ${step.name}`);
          step.status = 'FAILED';
        }
      } catch (e: any) {
        console.error(`[CRITICAL] Compensation failed for step ${step.name}:`, e.message);
        step.status = 'FAILED';
      }
    }

    const finalStatus: SagaStatus = compensatedStepsCount === currentStepIndex + 1 ? 'COMPENSATED' : 'FAILED';
    this.logEvent(sagaId, `Saga finished with status: ${finalStatus}`);

    return {
      sagaId,
      status: finalStatus,
      executedStepsCount: currentStepIndex + 1,
      compensatedStepsCount,
      timeline: this.timeline,
      error: sagaError,
    };
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  Saga Factories
  // #region Factories
  // ═══════════════════════════════════════════════════════════════════════════

  /**
   * createCrossMarketArbitrageSaga
   * 
   * Builds atomic dual execution saga:
   * Leg 1: Submit Polymarket Bet
   * Leg 2: Submit hedging TradFi Buy Order
   * If TradFi hedge order fails (e.g. rejection or lack of liquidity),
   * rolls back Leg 1 by submitting the Polymarket reversal trade.
   */
  public createCrossMarketArbitrageSaga(
    web3Sim: { simulateBet: () => Promise<boolean>; compensateBet: () => Promise<boolean> },
    tradFiSim: { submitOrder: () => Promise<boolean>; compensateOrder: () => Promise<boolean> }
  ): SagaStep[] {
    return [
      {
        name: 'POLYMARKET_EXECUTION_LEG',
        status: 'PENDING',
        execute: async () => {
          console.log('[SAGA-FACTORY] Simulating Web3 Bet leg...');
          return await web3Sim.simulateBet();
        },
        compensate: async () => {
          console.log('[SAGA-FACTORY] Reversing Web3 Bet leg...');
          return await web3Sim.compensateBet();
        }
      },
      {
        name: 'TRADFI_HEDGING_LEG',
        status: 'PENDING',
        execute: async () => {
          console.log('[SAGA-FACTORY] Submitting TradFi Hedge order...');
          return await tradFiSim.submitOrder();
        },
        compensate: async () => {
          console.log('[SAGA-FACTORY] Reversing TradFi Hedge order...');
          return await tradFiSim.compensateOrder();
        }
      }
    ];
  }

  /**
   * createTradFiBuySaga
   */
  public createTradFiBuySaga(
    submitFn: () => Promise<boolean>,
    compensateFn: () => Promise<boolean>
  ): SagaStep[] {
    return [
      {
        name: 'TRADFI_RESERVE_CAPITAL',
        status: 'PENDING',
        execute: async () => {
          console.log('[SAGA-FACTORY] Reserving capital...');
          return true;
        },
        compensate: async () => true,
      },
      {
        name: 'TRADFI_SUBMIT_ORDER',
        status: 'PENDING',
        execute: submitFn,
        compensate: compensateFn,
      }
    ];
  }
  // #endregion
}

// Simple test block to show rollback
if (process.argv[1]?.endsWith('saga_manager.js') || process.argv[1]?.endsWith('saga_manager.ts')) {
  (async () => {
    const orchestrator = new SagaOrchestrator();

    console.log('--- TEST 1: Successful Saga Execution ---');
    const stepsSuccess = orchestrator.createCrossMarketArbitrageSaga(
      { simulateBet: async () => true, compensateBet: async () => true },
      { submitOrder: async () => true, compensateOrder: async () => true }
    );
    await orchestrator.executeSaga(stepsSuccess);

    console.log('\n--- TEST 2: Saga Rollback (Leg 2 fails) ---');
    const stepsFail = orchestrator.createCrossMarketArbitrageSaga(
      { simulateBet: async () => true, compensateBet: async () => true },
      { submitOrder: async () => false, compensateOrder: async () => true } // Fails
    );
    await orchestrator.executeSaga(stepsFail);
  })();
}

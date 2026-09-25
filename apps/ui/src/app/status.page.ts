import { HttpClient } from '@angular/common/http';
import { Component, OnDestroy, OnInit } from '@angular/core';
import { Subscription, interval, switchMap } from 'rxjs';

@Component({
  selector: 'app-status',
  standalone: true,
  template: `
    <h1>Pipeline status</h1>
    <div class="grid">
      @if (s) {
      <div class="card">
        <h2>Health</h2>
        <p [class]="s.health === 'green' ? 'ok' : 'bad'">{{ s.health }}</p>
      </div>
      <div class="card">
        <h2>Backfill position</h2>
        <p>{{ s.backfill.cursor_id }} / {{ s.backfill.max_id }}</p>
        <small>{{ s.backfill.percent }}%</small>
      </div>
      <div class="card">
        <h2>Throughput</h2>
        <p>{{ s.throughput_per_sec }} /s</p>
      </div>
      <div class="card">
        <h2>Incremental lag</h2>
        <p>{{ s.incremental.lag_count }}</p>
        <small>{{ s.incremental.lag_seconds }}s</small>
      </div>
      <div class="card">
        <h2>DLQ depth</h2>
        <p>{{ s.dlq_open }}</p>
      </div>
      <div class="card">
        <h2>Circuit</h2>
        <p>{{ s.circuit.open ? 'open' : 'closed' }}</p>
      </div>
      }
    </div>
    <p>Guarantee: effectively-once (at-least-once + idempotent sinks).</p>
  `,
})
export class StatusPage implements OnInit, OnDestroy {
  s: any;
  private sub?: Subscription;
  constructor(private http: HttpClient) {}
  ngOnInit() {
    this.sub = interval(2000)
      .pipe(switchMap(() => this.http.get<any>('/api/status')))
      .subscribe((s) => (this.s = s));
    this.http.get<any>('/api/status').subscribe((s) => (this.s = s));
  }
  ngOnDestroy() {
    this.sub?.unsubscribe();
  }
}

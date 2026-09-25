import { HttpClient } from '@angular/common/http';
import { Component } from '@angular/core';

@Component({
  selector: 'app-simulate',
  standalone: true,
  template: `
    <h1>Simulation</h1>
    <p>These toggles fail inside the pipeline. Gate checks still stop real containers.</p>
    <div class="row">
      <button (click)="sink(true)">Search sink down</button>
      <button (click)="sink(false)">Search sink up</button>
    </div>
    <div class="row">
      <button (click)="poison()">Inject poison record</button>
      <button (click)="changes()">Generate source changes</button>
    </div>
    <p>{{ message }}</p>
  `,
})
export class SimulatePage {
  message = '';
  constructor(private http: HttpClient) {}
  sink(down: boolean) {
    this.http.post('/api/simulate/search-sink', { down }).subscribe((r) => (this.message = JSON.stringify(r)));
  }
  poison() {
    this.http.post('/api/simulate/poison', {}).subscribe((r) => (this.message = JSON.stringify(r)));
  }
  changes() {
    this.http.post('/api/simulate/changes', { count: 25 }).subscribe((r) => (this.message = JSON.stringify(r)));
  }
}

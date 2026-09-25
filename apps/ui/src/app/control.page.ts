import { HttpClient } from '@angular/common/http';
import { Component, OnInit } from '@angular/core';

@Component({
  selector: 'app-control',
  standalone: true,
  template: `
    <h1>Control</h1>
    <div class="row">
      <button (click)="post('/api/control/backfill/start')">Start backfill</button>
      <button (click)="post('/api/control/backfill/stop')">Stop backfill</button>
      <button (click)="post('/api/control/incremental/start')">Start incremental</button>
      <button (click)="post('/api/control/incremental/stop')">Stop incremental</button>
    </div>
    <div class="row">
      <label>Batch size <input #batch type="number" value="500" /></label>
      <button (click)="settings(batch.value)">Apply settings</button>
    </div>
    <p>{{ message }}</p>
    <h2>DLQ</h2>
    <table>
      <thead>
        <tr><th>Id</th><th>Record</th><th>Version</th><th>Batch</th><th>Error</th><th>Status</th><th></th></tr>
      </thead>
      <tbody>
        @for (row of dlq; track row.id) {
          <tr>
            <td>{{ row.id }}</td>
            <td>{{ row.record_id }}</td>
            <td>{{ row.version }}</td>
            <td>{{ row.batch_id }}</td>
            <td>{{ row.error }}</td>
            <td>{{ row.status }}</td>
            <td><button (click)="replay(row.id)" [disabled]="row.status !== 'open'">Replay</button></td>
          </tr>
        }
      </tbody>
    </table>
  `,
})
export class ControlPage implements OnInit {
  dlq: any[] = [];
  message = '';
  constructor(private http: HttpClient) {}
  ngOnInit() {
    this.load();
  }
  load() {
    this.http.get<any[]>('/api/dlq').subscribe((rows) => (this.dlq = rows ?? []));
  }
  post(url: string) {
    this.http.post(url, {}).subscribe((r) => {
      this.message = JSON.stringify(r);
      this.load();
    });
  }
  settings(batchSize: string) {
    this.http.post('/api/control/settings', { batchSize: Number(batchSize) }).subscribe((r) => {
      this.message = JSON.stringify(r);
    });
  }
  replay(id: number) {
    this.http.post('/api/dlq/' + id + '/replay', {}).subscribe((r) => {
      this.message = JSON.stringify(r);
      this.load();
    });
  }
}

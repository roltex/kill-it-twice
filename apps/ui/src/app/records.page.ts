import { JsonPipe } from '@angular/common';
import { HttpClient } from '@angular/common/http';
import { Component, OnDestroy, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Subscription, interval, switchMap } from 'rxjs';

@Component({
  selector: 'app-records',
  standalone: true,
  imports: [FormsModule, JsonPipe],
  template: `
    <h1>Replicated records</h1>
    <div class="row">
      <input [(ngModel)]="q" placeholder="Search email or name" />
      <button (click)="search()">Search</button>
    </div>
    <table>
      <thead>
        <tr><th>Id</th><th>Name</th><th>Email</th><th>Version</th><th></th></tr>
      </thead>
      <tbody>
        @for (item of items; track item.id) {
          <tr>
            <td>{{ item.id }}</td>
            <td>{{ item.name }}</td>
            <td>{{ item.email }}</td>
            <td>{{ item.version }}</td>
            <td><button (click)="open(item.id)">Details</button></td>
          </tr>
        }
      </tbody>
    </table>
    @if (detail) {
      <h2>Detail {{ detailId }}</h2>
      <pre>{{ detail | json }}</pre>
    }
    <h2>Live changes</h2>
    <table>
      <thead><tr><th>When</th><th>Record</th><th>Version</th><th>Mode</th></tr></thead>
      <tbody>
        @for (c of changes; track c.at + c.record_id) {
          <tr>
            <td>{{ c.at }}</td>
            <td>{{ c.record_id }}</td>
            <td>{{ c.version }}</td>
            <td>{{ c.mode }}</td>
          </tr>
        }
      </tbody>
    </table>
  `,
})
export class RecordsPage implements OnInit, OnDestroy {
  q = '';
  items: any[] = [];
  changes: any[] = [];
  detail: unknown = null;
  detailId = 0;
  private sub?: Subscription;
  constructor(private http: HttpClient) {}
  ngOnInit() {
    this.search();
    this.sub = interval(2000)
      .pipe(switchMap(() => this.http.get<any>('/api/changes')))
      .subscribe((r) => (this.changes = r.items ?? []));
  }
  ngOnDestroy() {
    this.sub?.unsubscribe();
  }
  search() {
    const params = this.q ? { q: this.q } : undefined;
    this.http.get<any>('/api/records', { params }).subscribe((r) => (this.items = r.items ?? []));
  }
  open(id: number) {
    this.detailId = id;
    this.http.get('/api/records/' + id).subscribe((d) => (this.detail = d));
  }
}

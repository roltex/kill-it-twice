import { Routes } from '@angular/router';
import { StatusPage } from './status.page';
import { RecordsPage } from './records.page';
import { ControlPage } from './control.page';
import { SimulatePage } from './simulate.page';

export const routes: Routes = [
  { path: '', component: StatusPage },
  { path: 'records', component: RecordsPage },
  { path: 'control', component: ControlPage },
  { path: 'simulate', component: SimulatePage },
];

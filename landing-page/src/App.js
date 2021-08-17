import React from 'react';
import './App.css';
import 'bootstrap/dist/css/bootstrap.min.css';
import UserFields from './UserFields'

class App extends React.Component {

  render() {
    return (
      <div>
        <div className='home__background-divider'></div>
        <div style={{display: 'flex', justifyContent: 'center', alignItems: 'center', marginTop: '100px'}}>
          <h1 style={{color: 'white', fontFamily: 'monospace'}}>Nautilus Update Request Page</h1>
        </div>
        <div style={{display: 'flex', justifyContent: 'center', alignItems: 'center', marginTop: '150px'}}>
          <UserFields/>
        </div>
      </div>
    );
  }
}

export default App;

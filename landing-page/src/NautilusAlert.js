import React from 'react';
import { Alert } from 'react-bootstrap';


class NautilusAlert extends React.Component {

    render() {
        return (
            <>
            {this.props.show ?
                <Alert variant="danger" onClose={this.props.handleClose} dismissible>
                    <Alert.Heading>Oops, looks like we're having some trouble handling this request. We'll let you know when the issue is resolved.</Alert.Heading>
                </Alert>
                : <div/>}
            </>
        );
    }
}

export default NautilusAlert;
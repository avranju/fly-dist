const topology = {
    n4: ['n1', 'n5', 'n3'],
    n5: ['n2', 'n4'],
    n1: ['n4', 'n2', 'n0'],
    n6: ['n3'],
    n3: ['n6', 'n0', 'n4'],
    n0: ['n3', 'n1'],
    n2: ['n5', 'n1'],
};

function main() {
    for (node of Object.keys(topology)) {
        for (neighbour of topology[node]) {
            console.log(`${node} -> ${neighbour};`);
        }
    }
}

main();

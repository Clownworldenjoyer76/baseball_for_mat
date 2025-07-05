
function calculateReturn() {
    const stake = parseFloat(document.getElementById('stake').value);
    const odds = parseFloat(document.getElementById('odds').value);
    if (!isNaN(stake) && !isNaN(odds)) {
        const potentialReturn = (stake * odds).toFixed(2);
        document.getElementById('return').innerText = "Potential return: $" + potentialReturn;
    } else {
        document.getElementById('return').innerText = "Please enter valid numbers.";
    }
}

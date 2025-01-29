import sys

from kodo.common import Launch
from kodo.common import publish


def execute(inputs, event):
    return "OK"


flow = publish(
    execute, 
    url="/test/flow", 
    name="Experimental Test Flow",
    description="Technical Test Flow"
)


@flow.enter
def landing_page(form, method):
    if form.get("submit"):
        if form.get("terms") == "agree":
            return Launch()
    doc = f"""
    <h1>
        Disclaimer / Terms & Conditions
    </h1>

    { '<p class="error">If you want to use this service agree to the Terms & Conditions below.</p>' if form.get('submit') else ""}

    <p>
        Welcome to the Agentic System. Please read the following terms and conditions carefully before using this system. By accessing or using the Agentic System, you agree to be bound by these terms.
    </p>

    <ol>
        <li>
            <p>
                <strong>Experimental Nature</strong>: 
                The Agentic System is an experimental platform. It is provided "as is" without any guarantees or warranties of any kind. The system is still under development and may contain errors or inaccuracies.
            </p>
        </li>
        <li>
            <p>
                <strong>Use at Your Own Risk</strong>: 
                Users are advised to use the Agentic System at their own risk. The developers and providers of this system are not liable for any damages or losses that may arise from its use, including but not limited to data loss, system failures, or any other issues.
            </p>
        </li>
        <li>
            <p>
                <strong>No Liability</strong>: 
                The creators and operators of the Agentic System disclaim any liability for any harm or damages resulting from the use of the system. Users assume full responsibility for any actions taken based on the information provided by the system.
            </p>
        </li>
        <li>
            <p>
                <strong>No Warranty</strong>: 
                There are no warranties, express or implied, regarding the performance or reliability of the Agentic System. This includes, but is not limited to, implied warranties of merchantability, fitness for a particular purpose, or non-infringement.
            </p>
        </li>
    </ol>
        
    <blockquote>
        <p>
            By using the Agentic System, you acknowledge that you have read, understood, and agreed to these terms and conditions. If you do not agree with any part of these terms, you should not use the system.
        </p>
        <p>
            <label>
                <input type="checkbox" name="terms" value="agree">
                    I agree to the <a href="#">terms and conditions</a>
            </label>
        </p>
        <p>
            <input type="submit" name="submit" value="Start">
        </p>
    </blockquote>
    """
    print("THIS IS STDOUT")
    print("THIS IS STDERR", file=sys.stderr)
    from subprocess import check_call
    check_call([sys.executable, "-c", "import sys; print('This is stdout'); print('This is stderr', file=sys.stderr)"])
    return doc

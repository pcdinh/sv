# -*- coding: utf-8 -*-

if __name__ == '__main__':
    from start_app import app
    import routes
    from datetime import datetime
    print("Starting at {}".format(datetime.now()))
    app.run(debug=True, host='0.0.0.0', port=8000, workers=1)

from flask import Blueprint

# from controllers.Trip_verification_controller import tvr_process, trip_Filter_process
from controllers.Trip_verification_controller import tvr_process, trip_Filter_process

tvr_bp = Blueprint('tvr_bp', __name__)

tvr_bp.route('/tvrMail', methods=['GET', 'POST'])(tvr_process)
tvr_bp.route('/tripFilterMail', methods=['GET', 'POST'])(trip_Filter_process)
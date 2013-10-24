$(function(){
	$("li").on("click",function(){
		$("li").removeClass("active");
		$(this).addClass("active");
	});
	$(window).bind('hashchange', function(e) {
		var url = $.param.fragment();
		if(url ==''){
			url = "dashboard";
		}
		$("#main-content").load(url);
	});
	$(window).trigger( 'hashchange' );
});
